"""Building Simulator (test harness, not one of the 8 services).

Simulates the physical building: each zone's temperature evolves with the shared thermal
model and reacts to the HVAC mode/power currently applied by the HVAC Controller, so the
control loop is a real closed feedback loop. Occupancy follows a time-of-day pattern.
Every SIM_INTERVAL_SECONDS it POSTs one reading per sensor to the Sensor Ingestion API.
"""
import logging
import math
import random
import threading
import time
from contextlib import asynccontextmanager

import httpx
from pydantic import BaseModel, Field

from sbcommon.config import env, env_float, env_int
from sbcommon.logs import setup_logging
from sbcommon.thermal import step
from sbcommon.web import create_app

log = setup_logging()

REGISTRY_URL = env("REGISTRY_URL", "http://localhost:8001")
INGEST_URL = env("INGEST_URL", "http://localhost:8002")
HVAC_URL = env("HVAC_URL", "http://localhost:8006")
INTERVAL_S = env_float("SIM_INTERVAL_SECONDS", 5)
SPEEDUP = env_float("SIM_SPEEDUP", 6.0)   # simulated minutes per real minute
SENSORS_PER_ZONE = env_int("SIM_SENSORS_PER_ZONE", 2)
_outside = {"c": env_float("OUTSIDE_TEMP_C", 32.0)}
_overrides: dict[str, float] = {}
_zones: dict[str, dict] = {}
_stop = threading.Event()


def _occupancy(zone_id: str, now: float) -> int:
    hour = (time.localtime(now).tm_hour + time.localtime(now).tm_min / 60.0)
    base = max(0.0, math.sin((hour - 7) / 14 * math.pi)) * 25 if 7 <= hour <= 21 else 0.0
    if "server" in zone_id or "archives" in zone_id:
        base *= 0.1
    return max(0, int(random.gauss(base, 2)))


def _tick(client: httpx.Client):
    zones = client.get(f"{REGISTRY_URL}/api/zones").json()
    if not zones:
        client.post(f"{REGISTRY_URL}/api/admin/seed").raise_for_status()
        zones = client.get(f"{REGISTRY_URL}/api/zones").json()
    units = {u["zone_id"]: u for u in client.get(f"{HVAC_URL}/api/hvac/units").json()}
    minutes = INTERVAL_S / 60.0 * SPEEDUP
    now = time.time()
    readings = []
    for z in zones:
        zid = z["zone_id"]
        st = _zones.setdefault(zid, {"temp": z["target_temp_c"] + random.uniform(2, 5), "occ": 0})
        st["occ"] = _occupancy(zid, now)
        u = units.get(zid, {"mode": "OFF", "power_pct": 0})
        st["temp"] = step(st["temp"], _outside["c"], st["occ"], u["mode"], u["power_pct"], minutes)
        if zid in _overrides:
            st["temp"] = _overrides.pop(zid)
        for i in range(SENSORS_PER_ZONE):
            readings.append({
                "sensor_id": f"{zid}-s{i + 1}", "zone_id": zid, "building_id": z["building_id"],
                "temperature_c": round(st["temp"] + random.gauss(0, 0.1), 2),
                "occupancy_count": st["occ"] if i == 0 else None,
                "humidity_pct": round(random.uniform(40, 60), 1),
                "ts_ms": int(now * 1000)})
    client.post(f"{INGEST_URL}/api/readings", json={"readings": readings}).raise_for_status()
    return len(readings)


def _loop():
    with httpx.Client(timeout=10) as client:
        while not _stop.wait(INTERVAL_S):
            try:
                n = _tick(client)
                log.debug("sent readings", extra={"count": n})
            except Exception as exc:  # noqa: BLE001
                log.warning("simulation tick failed", extra={"error": str(exc)})


@asynccontextmanager
async def lifespan(_app):
    threading.Thread(target=_loop, name="simulator", daemon=True).start()
    yield
    _stop.set()


app = create_app("Building Simulator", "/api/sim", lifespan=lifespan)


class Heatwave(BaseModel):
    outside_c: float = Field(ge=-30, le=60)


class Spike(BaseModel):
    temperature_c: float = Field(ge=-40, le=80)


@app.get("/api/sim/state")
def state():
    return {"outside_c": _outside["c"], "zones": _zones}


@app.put("/api/sim/outside")
def set_outside(h: Heatwave):
    _outside["c"] = h.outside_c
    return state()


@app.post("/api/sim/zones/{zone_id}/spike")
def spike(zone_id: str, s: Spike):
    """Force an abnormal temperature in one zone (demo for ABNORMAL_TEMPERATURE alert)."""
    _overrides[zone_id] = s.temperature_c
    return {"zone_id": zone_id, "next_temperature_c": s.temperature_c}
