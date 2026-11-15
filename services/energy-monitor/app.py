"""Energy Monitor - tracks simulated energy consumption against the building's power budget.

* 2PC participant (gRPC): Prepare reserves power budget for a zone's new HVAC load,
  Commit makes it the zone's committed load, Abort releases the reservation.
* Every ENERGY_TICK_SECONDS it emits `energy_sample` events (kWh per zone) to Kafka
  topic `energy-events` (consumed by the Flink Energy Analytics job).
* Consumes the Flink job's aggregated results from `energy-analytics-results`, stores
  them in Firestore and raises alerts on control instability / energy spikes.
"""
import logging
import threading
import time
from contextlib import asynccontextmanager

from fastapi import Query
from prometheus_client import Gauge
from pydantic import BaseModel, Field

from sbcommon.config import (SERVICE_NAME, TOPIC_ANALYTICS_RESULTS, TOPIC_ENERGY_EVENTS, env_float,
                             env_int)
from sbcommon.db import create_pool, migrate
from sbcommon.gcp import AlertPublisher, firestore_client
from sbcommon.grpcutil import TWOPC_PARTICIPANT_CALLS, start_participant_server
from sbcommon.kafka import JsonConsumerLoop, JsonProducer
from sbcommon.logs import setup_logging
from sbcommon.web import create_app
from sbproto import twopc_pb2, twopc_pb2_grpc

log = setup_logging()

DEFAULT_BUDGET_KW = env_float("ENERGY_BUDGET_KW", 45.0)
TICK_S = env_int("ENERGY_TICK_SECONDS", 10)
SPIKE_KWH_PER_MIN = env_float("ENERGY_SPIKE_KWH_PER_MIN", 0.165)  # ~10 kW (full power) for a whole minute
FLIPS_UNSTABLE = env_int("CONTROL_FLIPS_UNSTABLE", 3)

DDL = f"""
CREATE TABLE IF NOT EXISTS energy_budget (
    id          INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    budget_kw   DOUBLE PRECISION NOT NULL CHECK (budget_kw > 0),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO energy_budget(id, budget_kw) VALUES (1, {DEFAULT_BUDGET_KW}) ON CONFLICT (id) DO NOTHING;
CREATE TABLE IF NOT EXISTS zone_load (
    zone_id     TEXT PRIMARY KEY,
    kw          DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS energy_txns (
    txn_id      TEXT PRIMARY KEY,
    zone_id     TEXT,
    kw          DOUBLE PRECISION,
    state       TEXT NOT NULL,
    vote        BOOLEAN,
    reason      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS energy_txns_state_idx ON energy_txns(state);
CREATE TABLE IF NOT EXISTS energy_ledger (
    id          BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    total_kw    DOUBLE PRECISION NOT NULL,
    kwh         DOUBLE PRECISION NOT NULL
);
"""

TOTAL_KW = Gauge("energy_total_load_kw", "Committed HVAC electrical load (kW)")
BUDGET_KW = Gauge("energy_budget_kw", "Configured building power budget (kW)")
RESERVED_KW = Gauge("energy_reserved_kw", "Power reserved by prepared 2PC transactions (kW)")

pool = None
producer = None
alerts = None
consumer = None
grpc_server = None
_stop = threading.Event()


class EnergyParticipant(twopc_pb2_grpc.ParticipantServicer):
    def Prepare(self, request, context):
        kw = max(0.0, float(request.power_kw))
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # serialise all reservations against the single budget row
                cur.execute("SELECT budget_kw FROM energy_budget WHERE id=1 FOR UPDATE")
                budget = cur.fetchone()["budget_kw"]
                cur.execute("SELECT * FROM energy_txns WHERE txn_id=%s", (request.txn_id,))
                existing = cur.fetchone()
                if existing:
                    conn.commit()
                    yes = bool(existing["vote"]) and existing["state"] == "PREPARED"
                    return twopc_pb2.Vote(txn_id=request.txn_id, yes=yes, participant=SERVICE_NAME,
                                          reason=existing["reason"] or existing["state"])
                cur.execute("SELECT count(*) AS n FROM energy_txns WHERE zone_id=%s AND state='PREPARED'",
                            (request.zone_id,))
                inflight = cur.fetchone()["n"]
                cur.execute("SELECT COALESCE(sum(kw),0) AS s FROM zone_load WHERE zone_id<>%s", (request.zone_id,))
                committed_other = cur.fetchone()["s"]
                cur.execute("SELECT COALESCE(sum(kw),0) AS s FROM energy_txns WHERE state='PREPARED' AND zone_id<>%s",
                            (request.zone_id,))
                reserved_other = cur.fetchone()["s"]
                projected = committed_other + reserved_other + kw
                if inflight:
                    yes, reason = False, "zone already has an in-flight transaction"
                elif projected > budget + 1e-9:
                    yes, reason = False, f"budget exceeded: projected {projected:.1f} kW > budget {budget:.1f} kW"
                else:
                    yes, reason = True, f"reserved {kw:.1f} kW (projected {projected:.1f}/{budget:.1f} kW)"
                cur.execute("INSERT INTO energy_txns(txn_id,zone_id,kw,state,vote,reason) VALUES (%s,%s,%s,%s,%s,%s)",
                            (request.txn_id, request.zone_id, kw, "PREPARED" if yes else "ABORTED", yes, reason))
            conn.commit()
        TWOPC_PARTICIPANT_CALLS.labels(SERVICE_NAME, "prepare", "yes" if yes else "no").inc()
        log.info("2PC prepare", extra={"txn_id": request.txn_id, "zone_id": request.zone_id,
                                       "vote": "YES" if yes else "NO", "reason": reason})
        return twopc_pb2.Vote(txn_id=request.txn_id, yes=yes, participant=SERVICE_NAME, reason=reason)

    def Commit(self, request, context):
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM energy_txns WHERE txn_id=%s FOR UPDATE", (request.txn_id,))
                t = cur.fetchone()
                if t is None:
                    state = "UNKNOWN"
                    log.error("2PC commit for unknown txn", extra={"txn_id": request.txn_id})
                elif t["state"] == "PREPARED":
                    cur.execute("INSERT INTO zone_load(zone_id,kw) VALUES (%s,%s) ON CONFLICT (zone_id) "
                                "DO UPDATE SET kw=EXCLUDED.kw, updated_at=now()", (t["zone_id"], t["kw"]))
                    cur.execute("UPDATE energy_txns SET state='COMMITTED', decided_at=now() WHERE txn_id=%s",
                                (request.txn_id,))
                    state = "COMMITTED"
                else:
                    state = t["state"]
            conn.commit()
        TWOPC_PARTICIPANT_CALLS.labels(SERVICE_NAME, "commit", state.lower()).inc()
        log.info("2PC commit", extra={"txn_id": request.txn_id, "state": state})
        return twopc_pb2.Ack(txn_id=request.txn_id, participant=SERVICE_NAME, state=state)

    def Abort(self, request, context):
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM energy_txns WHERE txn_id=%s FOR UPDATE", (request.txn_id,))
                t = cur.fetchone()
                if t is None:
                    cur.execute("INSERT INTO energy_txns(txn_id,state,vote,reason,decided_at) "
                                "VALUES (%s,'ABORTED',false,'aborted before prepare',now()) ON CONFLICT DO NOTHING",
                                (request.txn_id,))
                    state = "ABORTED"
                elif t["state"] == "PREPARED":
                    cur.execute("UPDATE energy_txns SET state='ABORTED', decided_at=now() WHERE txn_id=%s",
                                (request.txn_id,))
                    state = "ABORTED"
                else:
                    state = t["state"]
            conn.commit()
        TWOPC_PARTICIPANT_CALLS.labels(SERVICE_NAME, "abort", state.lower()).inc()
        log.info("2PC abort", extra={"txn_id": request.txn_id, "state": state})
        return twopc_pb2.Ack(txn_id=request.txn_id, participant=SERVICE_NAME, state=state)


def _meter_loop():
    """Simulated smart meter: every tick convert committed load into kWh samples per zone."""
    while not _stop.wait(TICK_S):
        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    # only one replica meters at a time
                    cur.execute("SELECT pg_try_advisory_xact_lock(777200) AS ok")
                    if not cur.fetchone()["ok"]:
                        conn.commit()
                        continue
                    cur.execute("SELECT zone_id, kw FROM zone_load")
                    loads = cur.fetchall()
                    cur.execute("SELECT budget_kw FROM energy_budget WHERE id=1")
                    budget = cur.fetchone()["budget_kw"]
                    cur.execute("SELECT COALESCE(sum(kw),0) AS s FROM energy_txns WHERE state='PREPARED'")
                    reserved = cur.fetchone()["s"]
                    total = sum(r["kw"] for r in loads)
                    cur.execute("INSERT INTO energy_ledger(total_kw,kwh) VALUES (%s,%s)",
                                (total, total * TICK_S / 3600.0))
                conn.commit()
            TOTAL_KW.set(total)
            BUDGET_KW.set(budget)
            RESERVED_KW.set(reserved)
            now_ms = int(time.time() * 1000)
            for r in loads:
                producer.send(TOPIC_ENERGY_EVENTS, {
                    "event_type": "energy_sample", "zone_id": r["zone_id"], "power_kw": round(r["kw"], 3),
                    "kwh": round(r["kw"] * TICK_S / 3600.0, 6), "ts_ms": now_ms}, key=r["zone_id"])
        except Exception:  # noqa: BLE001
            log.exception("energy meter tick failed")


def handle_analytics(_topic, _key, result: dict):
    """Store the Flink window result in Firestore and evaluate it."""
    zone = result.get("zone_id")
    if not zone or "window_start_ms" not in result:
        log.warning("ignoring malformed analytics result", extra={"result": result})
        return
    db = firestore_client()
    doc_id = f"{zone}_{result['window_start_ms']}"
    db.collection("energy_analytics").document(doc_id).set(result)
    db.collection("zone_analytics_latest").document(zone).set(result)
    if result.get("mode_flips", 0) >= FLIPS_UNSTABLE:
        alerts.publish("CONTROL_INSTABILITY", "WARNING",
                       f"HVAC in zone {zone} changed mode {result['mode_flips']} times in one minute",
                       zone_id=zone, details=result)
    if result.get("energy_kwh", 0) > SPIKE_KWH_PER_MIN:
        alerts.publish("ENERGY_SPIKE", "WARNING",
                       f"Zone {zone} used {result['energy_kwh']:.2f} kWh in one minute", zone_id=zone, details=result)


@asynccontextmanager
async def lifespan(_app):
    global pool, producer, alerts, consumer, grpc_server
    pool = create_pool()
    migrate(pool, DDL)
    producer = JsonProducer()
    alerts = AlertPublisher()
    grpc_server = start_participant_server(EnergyParticipant(), env_int("GRPC_PORT", 50052))
    consumer = JsonConsumerLoop([TOPIC_ANALYTICS_RESULTS], "energy-monitor", handle_analytics)
    consumer.start()
    threading.Thread(target=_meter_loop, name="energy-meter", daemon=True).start()
    yield
    _stop.set()
    consumer.stop()
    grpc_server.stop(grace=5)
    producer.close()
    pool.close()


app = create_app("Energy Monitor", "/api/energy", lifespan=lifespan)


class BudgetIn(BaseModel):
    budget_kw: float = Field(gt=0, le=100000)


def _rows(sql, params=(), commit=False):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() if cur.description else []
        conn.commit()
    return rows


@app.get("/api/energy/budget")
def get_budget():
    return _rows("SELECT * FROM energy_budget WHERE id=1")[0]


@app.put("/api/energy/budget")
def set_budget(b: BudgetIn):
    """Lower the budget to demonstrate the Energy Monitor voting NO (2PC abort)."""
    row = _rows("UPDATE energy_budget SET budget_kw=%s, updated_at=now() WHERE id=1 RETURNING *", (b.budget_kw,))[0]
    log.warning("energy budget changed", extra={"budget_kw": b.budget_kw})
    return row


@app.get("/api/energy/load")
def current_load():
    zones = _rows("SELECT * FROM zone_load ORDER BY zone_id")
    budget = get_budget()["budget_kw"]
    reserved = _rows("SELECT COALESCE(sum(kw),0) AS s FROM energy_txns WHERE state='PREPARED'")[0]["s"]
    total = sum(z["kw"] for z in zones)
    kwh_today = _rows("SELECT COALESCE(sum(kwh),0) AS s FROM energy_ledger WHERE ts >= date_trunc('day', now())")[0]["s"]
    return {"budget_kw": budget, "total_kw": round(total, 3), "reserved_kw": round(reserved, 3),
            "headroom_kw": round(budget - total - reserved, 3), "kwh_today": round(kwh_today, 4), "zones": zones}


@app.get("/api/energy/transactions")
def participant_log(limit: int = Query(50, ge=1, le=500)):
    return _rows("SELECT * FROM energy_txns ORDER BY created_at DESC LIMIT %s", (limit,))


@app.get("/api/energy/analytics")
def analytics(zone_id: str | None = None, limit: int = Query(20, ge=1, le=200)):
    """Latest 1-minute window aggregates computed by the Flink job (read from Firestore)."""
    db = firestore_client()
    if zone_id is None:
        return [d.to_dict() for d in db.collection("zone_analytics_latest").stream()]
    from google.cloud import firestore

    q = (db.collection("energy_analytics").where(filter=firestore.FieldFilter("zone_id", "==", zone_id))
         .order_by("window_start_ms", direction=firestore.Query.DESCENDING).limit(limit))
    return [d.to_dict() for d in q.stream()]
