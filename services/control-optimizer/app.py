"""Control Optimizer - feedback-control decisions from the current sensor state.

Every CONTROL_INTERVAL_SECONDS (only one replica at a time, via a Postgres advisory lock):
  1. read zones (Building Registry, REST), live zone state (Occupancy Service, REST) and
     HVAC units (HVAC Controller, REST)
  2. run a model-predictive controller (thermal model, grid search) per zone
  3. if the best command differs from what the unit is doing, apply it atomically with
     Two-Phase Commit across HVAC Controller + Energy Monitor (gRPC)
Also exposes a CPU-bound /api/control/recommend endpoint (scaled by HPA under load).
"""
import logging
import threading
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import HTTPException, Query
from prometheus_client import Counter, Histogram
from pydantic import BaseModel, Field

from sbcommon.config import env, env_float, env_int
from sbcommon.db import create_pool, migrate
from sbcommon.gcp import AlertPublisher
from sbcommon.logs import setup_logging
from sbcommon.thermal import mpc_choose
from sbcommon.web import create_app
from twopc import DDL, Coordinator

log = setup_logging()

REGISTRY_URL = env("REGISTRY_URL", "http://localhost:8001")
OCCUPANCY_URL = env("OCCUPANCY_URL", "http://localhost:8003")
HVAC_URL = env("HVAC_URL", "http://localhost:8006")
ENERGY_URL = env("ENERGY_URL", "http://localhost:8004")
BUDGET_USE = env_float("BUDGET_USE_FRACTION", 0.95)   # keep 5 % headroom for operator commands
HVAC_GRPC = env("HVAC_GRPC", "localhost:50051")
ENERGY_GRPC = env("ENERGY_GRPC", "localhost:50052")
INTERVAL_S = env_int("CONTROL_INTERVAL_SECONDS", 20)
OUTSIDE_C = env_float("OUTSIDE_TEMP_C", 32.0)
STALE_S = env_int("STATE_STALE_SECONDS", 120)
CONTROL_LOOP = env("CONTROL_LOOP_ENABLED", "true").lower() == "true"
RECOVERY_AGE_S = env_int("TWOPC_RECOVERY_AGE_SECONDS", 20)
DEFAULT_RATED_KW = env_float("HVAC_DEFAULT_RATED_KW", 10.0)

DECISIONS = Counter("control_decisions_total", "Control decisions per cycle", ["action"])
CYCLE_TIME = Histogram("control_cycle_duration_seconds", "Duration of one control cycle",
                       buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30))

pool = None
coordinator: Coordinator | None = None
_stop = threading.Event()
_last_cycle: dict = {}


def _get(url):
    r = httpx.get(url, timeout=5)
    r.raise_for_status()
    return r.json()


def _fair_share(plans: list[dict], budget_kw: float, fixed_kw: float) -> None:
    """If the zones together want more power than the budget allows, scale every zone's request down
    proportionally (in 5 % steps) so that all zones get a fair share instead of one zone starving."""
    available = max(0.0, budget_kw * BUDGET_USE - fixed_kw)
    wanted = sum(p["best"]["power_kw"] for p in plans)
    if wanted <= available or wanted == 0:
        return
    factor = available / wanted
    for p in plans:
        b = p["best"]
        if b["mode"] == "OFF":
            continue
        pct = int(b["power_pct"] * factor // 5 * 5)
        b.update(power_pct=pct, power_kw=round(p["rated_kw"] * pct / 100.0, 3), budget_limited=True)
        if pct == 0:
            b["mode"] = "OFF"


def run_cycle() -> dict:
    started = time.perf_counter()
    zones = _get(f"{REGISTRY_URL}/api/zones")
    states = {s["zone_id"]: s for s in _get(f"{OCCUPANCY_URL}/api/occupancy")}
    units = {u["zone_id"]: u for u in _get(f"{HVAC_URL}/api/hvac/units")}
    budget_kw = _get(f"{ENERGY_URL}/api/energy/budget")["budget_kw"]
    now_ms = time.time() * 1000
    decisions, plans, fixed_kw = [], [], 0.0
    for z in zones:
        zid = z["zone_id"]
        unit = units.get(zid) or {"mode": "OFF", "power_pct": 0, "rated_kw": DEFAULT_RATED_KW, "status": "ONLINE"}
        current_kw = unit["rated_kw"] * unit["power_pct"] / 100.0 if unit["mode"] != "OFF" else 0.0
        s = states.get(zid)
        if not s or s.get("temperature_c") is None or now_ms - s.get("updated_at_ms", 0) > STALE_S * 1000:
            DECISIONS.labels("no_data").inc()
            decisions.append({"zone_id": zid, "action": "no_data"})
            fixed_kw += current_kw     # untouched zones keep their load
            continue
        occupied = bool(s.get("occupied"))
        band = 0.5 if occupied else 2.5          # wider comfort band when nobody is there (energy saving)
        best = mpc_choose(s["temperature_c"], z["target_temp_c"], band, s.get("occupancy_estimate", 0),
                          OUTSIDE_C, unit["rated_kw"])
        plans.append({"zone": z, "state": s, "unit": unit, "best": best, "occupied": occupied,
                      "rated_kw": unit["rated_kw"], "current_kw": current_kw})
    _fair_share(plans, budget_kw, fixed_kw)

    changes = []
    for p in plans:
        best, unit, zid = p["best"], p["unit"], p["zone"]["zone_id"]
        if best["mode"] == unit["mode"] and abs(best["power_pct"] - unit["power_pct"]) < 15:
            DECISIONS.labels("hold").inc()
            decisions.append({"zone_id": zid, "action": "hold", "temperature_c": p["state"]["temperature_c"],
                              "mode": unit["mode"], "power_pct": unit["power_pct"]})
        else:
            changes.append(p)
    # commit reductions first so the budget they free is available to the increases
    changes.sort(key=lambda p: p["best"]["power_kw"] - p["current_kw"])
    for p in changes:
        best, zid = p["best"], p["zone"]["zone_id"]
        result = coordinator.execute(zid, best["mode"], best["power_pct"], best["power_kw"], initiator="control-loop")
        DECISIONS.labels("change_" + result["outcome"].lower()).inc()
        decisions.append({"zone_id": zid, "action": "change", "temperature_c": p["state"]["temperature_c"],
                          "occupied": p["occupied"], "target_c": p["zone"]["target_temp_c"], "decision": best,
                          "txn_id": result["txn_id"], "outcome": result["outcome"], "reason": result["reason"]})
    elapsed = time.perf_counter() - started
    CYCLE_TIME.observe(elapsed)
    return {"at_ms": int(now_ms), "duration_ms": round(elapsed * 1000, 1), "budget_kw": budget_kw,
            "decisions": decisions}


def _with_leader_lock(key: int, fn):
    """Run fn only if this replica holds the advisory lock (one active controller cluster-wide)."""
    with pool.connection() as conn:
        conn.autocommit = True
        try:
            got = conn.execute("SELECT pg_try_advisory_lock(%s) AS ok", (key,)).fetchone()["ok"]
            if not got:
                return None
            try:
                return fn()
            finally:
                conn.execute("SELECT pg_advisory_unlock(%s)", (key,))
        finally:
            conn.autocommit = False


def _control_loop():
    global _last_cycle
    while not _stop.wait(INTERVAL_S):
        try:
            res = _with_leader_lock(777300, run_cycle)
            if res is not None:
                _last_cycle = res
                changes = [d for d in res["decisions"] if d["action"] == "change"]
                log.info("control cycle done", extra={"zones": len(res["decisions"]), "changes": len(changes),
                                                      "duration_ms": res["duration_ms"]})
        except Exception:  # noqa: BLE001
            log.exception("control cycle failed")


def _recovery_loop():
    time.sleep(5)
    while True:
        try:
            done = _with_leader_lock(777301, lambda: coordinator.recover(RECOVERY_AGE_S))
            if done:
                log.warning("recovered unfinished 2PC transactions", extra={"count": len(done)})
        except Exception:  # noqa: BLE001
            log.exception("2PC recovery failed")
        if _stop.wait(10):
            return


@asynccontextmanager
async def lifespan(_app):
    global pool, coordinator
    pool = create_pool(max_size=env_int("DB_POOL_MAX", 15))
    migrate(pool, DDL)
    coordinator = Coordinator(pool, {"hvac-controller": HVAC_GRPC, "energy-monitor": ENERGY_GRPC},
                              alerts=AlertPublisher())
    threading.Thread(target=_recovery_loop, name="twopc-recovery", daemon=True).start()
    if CONTROL_LOOP:
        threading.Thread(target=_control_loop, name="control-loop", daemon=True).start()
    yield
    _stop.set()
    pool.close()


app = create_app("Control Optimizer", "/api/control", lifespan=lifespan)


class CommandIn(BaseModel):
    mode: str = Field(pattern="^(HEAT|COOL|OFF)$")
    power_pct: int = Field(ge=0, le=100)
    inject: str | None = Field(default=None, pattern="^(crash_before_decision|crash_after_decision)$",
                               description="fault injection for the 2PC demo")


class RecommendIn(BaseModel):
    temperature_c: float = Field(ge=-20, le=60)
    target_c: float = Field(default=22, ge=10, le=35)
    occupancy: float = Field(default=0, ge=0, le=10000)
    outside_c: float = Field(default=OUTSIDE_C, ge=-30, le=60)
    rated_kw: float = Field(default=DEFAULT_RATED_KW, gt=0, le=1000)
    band_c: float = Field(default=0.5, ge=0, le=10)
    horizon_min: int = Field(default=60, ge=5, le=240)


@app.post("/api/control/zones/{zone_id}/command")
def manual_command(zone_id: str, cmd: CommandIn):
    """Operator override: apply a command to a zone through Two-Phase Commit."""
    try:
        unit = _get(f"{HVAC_URL}/api/hvac/units/by-zone/{zone_id}")
        rated = unit["rated_kw"]
    except httpx.HTTPError:
        # unit not registered yet, or HVAC Controller unreachable: still run 2PC - an unreachable
        # participant makes the transaction abort and raises a CONTROLLER_FAILURE alert.
        rated = DEFAULT_RATED_KW
    power = 0 if cmd.mode == "OFF" else cmd.power_pct
    return coordinator.execute(zone_id, cmd.mode, power, round(rated * power / 100.0, 3),
                               initiator="operator", inject=cmd.inject)


@app.post("/api/control/run")
def run_now():
    res = _with_leader_lock(777300, run_cycle)
    if res is None:
        raise HTTPException(409, "another replica is running a control cycle")
    return res


@app.post("/api/control/recommend")
def recommend(req: RecommendIn):
    """Pure model-predictive computation (no side effects). CPU-heavy: used for HPA load tests."""
    return mpc_choose(req.temperature_c, req.target_c, req.band_c, req.occupancy, req.outside_c,
                      req.rated_kw, horizon_min=req.horizon_min)


@app.get("/api/control/status")
def status():
    return {"interval_s": INTERVAL_S, "loop_enabled": CONTROL_LOOP, "last_cycle": _last_cycle}


@app.post("/api/control/recover")
def recover_now():
    done = _with_leader_lock(777301, lambda: coordinator.recover(RECOVERY_AGE_S))
    return {"recovered": done or []}


@app.get("/api/control/transactions")
def transactions(limit: int = Query(30, ge=1, le=500), state: str | None = None):
    with pool.connection() as conn:
        if state:
            rows = conn.execute("SELECT * FROM twopc_log WHERE state=%s ORDER BY created_at DESC LIMIT %s",
                                (state, limit)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM twopc_log ORDER BY created_at DESC LIMIT %s", (limit,)).fetchall()
        conn.commit()
    return rows


@app.get("/api/control/transactions/{txn_id}")
def transaction(txn_id: str):
    t = coordinator.get(txn_id)
    if not t:
        raise HTTPException(404, "transaction not found")
    return t
