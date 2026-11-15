"""HVAC Controller - applies simulated heating/cooling commands to HVAC units.

It is a 2PC *participant* (gRPC):
  Prepare -> lock the zone's HVAC unit (must be ONLINE and not locked by another txn), vote
  Commit  -> apply mode/power, release lock, emit an `hvac_command` event to Kafka
  Abort   -> release lock
Unit state + participant transaction log are stored in Cloud SQL (database `hvac`).
"""
import logging
import threading
import time
from contextlib import asynccontextmanager

from fastapi import HTTPException, Query
from prometheus_client import Gauge
from pydantic import BaseModel, Field

from sbcommon.config import SERVICE_NAME, TOPIC_ENERGY_EVENTS, env_float, env_int
from sbcommon.db import create_pool, migrate
from sbcommon.grpcutil import TWOPC_PARTICIPANT_CALLS, start_participant_server
from sbcommon.kafka import JsonProducer
from sbcommon.logs import setup_logging
from sbcommon.web import create_app
from sbproto import twopc_pb2, twopc_pb2_grpc

log = setup_logging()

DDL = """
CREATE TABLE IF NOT EXISTS hvac_units (
    unit_id          TEXT PRIMARY KEY,
    zone_id          TEXT UNIQUE NOT NULL,
    mode             TEXT NOT NULL DEFAULT 'OFF',
    power_pct        INT  NOT NULL DEFAULT 0,
    rated_kw         DOUBLE PRECISION NOT NULL,
    status           TEXT NOT NULL DEFAULT 'ONLINE',
    locked_by        TEXT,
    last_command_at  TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS hvac_txns (
    txn_id           TEXT PRIMARY KEY,
    unit_id          TEXT,
    zone_id          TEXT,
    mode             TEXT,
    power_pct        INT,
    requested_at_ms  BIGINT,
    state            TEXT NOT NULL,
    vote             BOOLEAN,
    reason           TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_at       TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS hvac_txns_created_idx ON hvac_txns(created_at DESC);
"""

DEFAULT_RATED_KW = env_float("HVAC_DEFAULT_RATED_KW", 10.0)
VALID_STATUS = ("ONLINE", "MAINTENANCE", "FAULT")
UNITS_LOCKED = Gauge("hvac_units_locked", "HVAC units currently locked by a prepared 2PC transaction")

pool = None
producer = None
grpc_server = None


def _ensure_unit(cur, zone_id: str):
    cur.execute("SELECT * FROM hvac_units WHERE zone_id=%s FOR UPDATE", (zone_id,))
    unit = cur.fetchone()
    if unit is None:
        # auto-register a unit the first time a zone is controlled
        cur.execute("INSERT INTO hvac_units(unit_id,zone_id,rated_kw) VALUES (%s,%s,%s) "
                    "ON CONFLICT (zone_id) DO NOTHING", (f"ahu-{zone_id}", zone_id, DEFAULT_RATED_KW))
        cur.execute("SELECT * FROM hvac_units WHERE zone_id=%s FOR UPDATE", (zone_id,))
        unit = cur.fetchone()
    return unit


class HvacParticipant(twopc_pb2_grpc.ParticipantServicer):
    def Prepare(self, request, context):
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM hvac_txns WHERE txn_id=%s FOR UPDATE", (request.txn_id,))
                existing = cur.fetchone()
                if existing:  # duplicate Prepare (retry) or Prepare arriving after Abort
                    conn.commit()
                    yes = bool(existing["vote"]) and existing["state"] == "PREPARED"
                    return twopc_pb2.Vote(txn_id=request.txn_id, yes=yes, participant=SERVICE_NAME,
                                          reason=existing["reason"] or existing["state"])
                unit = _ensure_unit(cur, request.zone_id)
                yes, reason = True, "unit locked for txn"
                if request.mode not in ("HEAT", "COOL", "OFF") or not 0 <= request.power_pct <= 100:
                    yes, reason = False, f"invalid command {request.mode}/{request.power_pct}"
                elif unit["status"] != "ONLINE":
                    yes, reason = False, f"unit {unit['unit_id']} is {unit['status']}"
                elif unit["locked_by"] and unit["locked_by"] != request.txn_id:
                    yes, reason = False, f"unit {unit['unit_id']} locked by txn {unit['locked_by']}"
                if yes:
                    cur.execute("UPDATE hvac_units SET locked_by=%s, updated_at=now() WHERE unit_id=%s",
                                (request.txn_id, unit["unit_id"]))
                cur.execute("INSERT INTO hvac_txns(txn_id,unit_id,zone_id,mode,power_pct,requested_at_ms,state,vote,reason) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (request.txn_id, unit["unit_id"], request.zone_id, request.mode, request.power_pct,
                             request.requested_at_ms, "PREPARED" if yes else "ABORTED", yes, reason))
            conn.commit()
        TWOPC_PARTICIPANT_CALLS.labels(SERVICE_NAME, "prepare", "yes" if yes else "no").inc()
        log.info("2PC prepare", extra={"txn_id": request.txn_id, "zone_id": request.zone_id,
                                       "vote": "YES" if yes else "NO", "reason": reason})
        return twopc_pb2.Vote(txn_id=request.txn_id, yes=yes, participant=SERVICE_NAME, reason=reason)

    def Commit(self, request, context):
        event = None
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM hvac_txns WHERE txn_id=%s FOR UPDATE", (request.txn_id,))
                t = cur.fetchone()
                if t is None:
                    conn.commit()
                    TWOPC_PARTICIPANT_CALLS.labels(SERVICE_NAME, "commit", "unknown").inc()
                    log.error("2PC commit for unknown txn", extra={"txn_id": request.txn_id})
                    return twopc_pb2.Ack(txn_id=request.txn_id, participant=SERVICE_NAME, state="UNKNOWN")
                if t["state"] == "PREPARED":
                    cur.execute("UPDATE hvac_units SET mode=%s, power_pct=%s, locked_by=NULL, "
                                "last_command_at=now(), updated_at=now() WHERE unit_id=%s RETURNING rated_kw",
                                (t["mode"], t["power_pct"], t["unit_id"]))
                    rated = cur.fetchone()["rated_kw"]
                    cur.execute("UPDATE hvac_txns SET state='COMMITTED', decided_at=now() WHERE txn_id=%s",
                                (request.txn_id,))
                    now_ms = int(time.time() * 1000)
                    event = {"event_type": "hvac_command", "zone_id": t["zone_id"], "unit_id": t["unit_id"],
                             "mode": t["mode"], "power_pct": t["power_pct"],
                             "power_kw": round(rated * t["power_pct"] / 100.0, 3),
                             "txn_id": t["txn_id"], "response_ms": max(0, now_ms - (t["requested_at_ms"] or now_ms)),
                             "ts_ms": now_ms}
                    state = "COMMITTED"
                else:
                    state = t["state"]  # idempotent repeat (COMMITTED) or protocol error (ABORTED)
                    if state == "ABORTED":
                        log.error("2PC commit received for aborted txn", extra={"txn_id": request.txn_id})
            conn.commit()
        if event:
            producer.send(TOPIC_ENERGY_EVENTS, event, key=event["zone_id"])
            log.info("HVAC command applied", extra={k: event[k] for k in ("txn_id", "zone_id", "mode", "power_pct",
                                                                          "response_ms")})
        TWOPC_PARTICIPANT_CALLS.labels(SERVICE_NAME, "commit", state.lower()).inc()
        return twopc_pb2.Ack(txn_id=request.txn_id, participant=SERVICE_NAME, state=state)

    def Abort(self, request, context):
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM hvac_txns WHERE txn_id=%s FOR UPDATE", (request.txn_id,))
                t = cur.fetchone()
                if t is None:
                    # tombstone: a Prepare that arrives late for this txn will vote NO
                    cur.execute("INSERT INTO hvac_txns(txn_id,state,vote,reason,decided_at) "
                                "VALUES (%s,'ABORTED',false,'aborted before prepare',now()) ON CONFLICT DO NOTHING",
                                (request.txn_id,))
                    state = "ABORTED"
                elif t["state"] == "PREPARED":
                    cur.execute("UPDATE hvac_units SET locked_by=NULL, updated_at=now() WHERE locked_by=%s",
                                (request.txn_id,))
                    cur.execute("UPDATE hvac_txns SET state='ABORTED', decided_at=now() WHERE txn_id=%s",
                                (request.txn_id,))
                    state = "ABORTED"
                else:
                    state = t["state"]
                    if state == "COMMITTED":
                        log.error("2PC abort received for committed txn", extra={"txn_id": request.txn_id})
            conn.commit()
        TWOPC_PARTICIPANT_CALLS.labels(SERVICE_NAME, "abort", state.lower()).inc()
        log.info("2PC abort", extra={"txn_id": request.txn_id, "state": state})
        return twopc_pb2.Ack(txn_id=request.txn_id, participant=SERVICE_NAME, state=state)


def _gauge_loop():
    while True:
        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT count(*) AS n FROM hvac_units WHERE locked_by IS NOT NULL")
                    UNITS_LOCKED.set(cur.fetchone()["n"])
                conn.commit()
        except Exception:  # noqa: BLE001
            pass
        time.sleep(10)


@asynccontextmanager
async def lifespan(_app):
    global pool, producer, grpc_server
    pool = create_pool()
    migrate(pool, DDL)
    producer = JsonProducer()
    grpc_server = start_participant_server(HvacParticipant(), env_int("GRPC_PORT", 50051))
    threading.Thread(target=_gauge_loop, daemon=True).start()
    yield
    grpc_server.stop(grace=5)
    producer.close()
    pool.close()


app = create_app("HVAC Controller", "/api/hvac", lifespan=lifespan)


class StatusIn(BaseModel):
    status: str = Field(pattern="^(ONLINE|MAINTENANCE|FAULT)$")


class UnitIn(BaseModel):
    zone_id: str = Field(pattern=r"^[a-z0-9-]{3,60}$")
    rated_kw: float = Field(default=DEFAULT_RATED_KW, gt=0, le=1000)


def _rows(sql, params=()):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        conn.commit()
    return rows


@app.get("/api/hvac/units")
def list_units():
    return _rows("SELECT * FROM hvac_units ORDER BY zone_id")


@app.post("/api/hvac/units", status_code=201)
def register_unit(u: UnitIn):
    rows = _rows("INSERT INTO hvac_units(unit_id,zone_id,rated_kw) VALUES (%s,%s,%s) "
                 "ON CONFLICT (zone_id) DO UPDATE SET rated_kw=EXCLUDED.rated_kw RETURNING *",
                 (f"ahu-{u.zone_id}", u.zone_id, u.rated_kw))
    return rows[0]


@app.get("/api/hvac/units/by-zone/{zone_id}")
def unit_for_zone(zone_id: str):
    rows = _rows("SELECT * FROM hvac_units WHERE zone_id=%s", (zone_id,))
    if not rows:
        raise HTTPException(404, "no HVAC unit for zone")
    return rows[0]


@app.put("/api/hvac/units/{unit_id}/status")
def set_status(unit_id: str, s: StatusIn):
    """Put a unit into MAINTENANCE/FAULT to demonstrate a participant voting NO in 2PC."""
    rows = _rows("UPDATE hvac_units SET status=%s, updated_at=now() WHERE unit_id=%s RETURNING *",
                 (s.status, unit_id))
    if not rows:
        raise HTTPException(404, "unit not found")
    log.warning("HVAC unit status changed", extra={"unit_id": unit_id, "status": s.status})
    return rows[0]


@app.get("/api/hvac/transactions")
def participant_log(limit: int = Query(50, ge=1, le=500)):
    return _rows("SELECT * FROM hvac_txns ORDER BY created_at DESC LIMIT %s", (limit,))
