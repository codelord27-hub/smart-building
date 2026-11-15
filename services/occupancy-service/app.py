"""Occupancy Service - estimates occupancy and current conditions per zone.

Consumes `sensor-readings` from Kafka, keeps an exponentially-weighted occupancy
estimate per zone, writes the zone state to Firestore (`zone_state/{zone_id}`, flushed
at most once per second per zone) and raises ABNORMAL_TEMPERATURE alerts via Pub/Sub
when a zone leaves the limits defined in the Building Registry.
"""
import logging
import os
import threading
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import HTTPException
from prometheus_client import Gauge

from sbcommon.config import TOPIC_SENSOR_READINGS, env, env_float
from sbcommon.gcp import AlertPublisher, firestore_client
from sbcommon.kafka import JsonConsumerLoop
from sbcommon.logs import setup_logging
from sbcommon.web import create_app

log = setup_logging()

REGISTRY_URL = env("REGISTRY_URL", "http://localhost:8001")
ALPHA = env_float("OCCUPANCY_EWMA_ALPHA", 0.3)
FLUSH_S = env_float("OCCUPANCY_FLUSH_SECONDS", 1.0)

ZONES_TRACKED = Gauge("occupancy_zones_tracked", "Zones with live sensor data")
ZONE_TEMP = Gauge("zone_temperature_celsius", "Latest temperature per zone", ["zone_id"])
ZONE_OCC = Gauge("zone_occupancy_estimate", "Estimated occupants per zone", ["zone_id"])

_state: dict[str, dict] = {}
_dirty: set[str] = set()
_lock = threading.Lock()
_limits: dict[str, dict] = {}
_limits_at = 0.0
_stop = threading.Event()
alerts = None
consumer = None


def _zone_limits(zone_id: str) -> dict | None:
    global _limits, _limits_at
    if time.time() - _limits_at > 60:
        try:
            r = httpx.get(f"{REGISTRY_URL}/api/zones", timeout=3)
            r.raise_for_status()
            _limits = {z["zone_id"]: z for z in r.json()}
            _limits_at = time.time()
        except Exception as exc:  # noqa: BLE001
            log.warning("could not refresh zone limits from registry", extra={"error": str(exc)})
            _limits_at = time.time() - 50  # retry in ~10 s
    return _limits.get(zone_id)


def handle_reading(_topic, _key, r: dict):
    zone = r.get("zone_id")
    if not zone:
        return
    temp = r.get("temperature_c")
    occ = r.get("occupancy_count")
    with _lock:
        s = _state.setdefault(zone, {"zone_id": zone, "building_id": r.get("building_id"),
                                     "occupancy_estimate": float(occ or 0), "readings_seen": 0})
        if temp is not None:
            s["temperature_c"] = round(float(temp), 2)
        if occ is not None:
            s["occupancy_count"] = int(occ)
            s["occupancy_estimate"] = round(ALPHA * float(occ) + (1 - ALPHA) * s["occupancy_estimate"], 2)
        s["occupied"] = s["occupancy_estimate"] >= 0.5
        s["readings_seen"] += 1
        s["updated_at_ms"] = int(r.get("ts_ms") or time.time() * 1000)
        _dirty.add(zone)
    if temp is not None:
        limits = _zone_limits(zone)
        if limits and not (limits["min_temp_c"] <= float(temp) <= limits["max_temp_c"]):
            alerts.publish("ABNORMAL_TEMPERATURE", "CRITICAL",
                           f"Zone {zone} at {float(temp):.1f}°C, outside limits "
                           f"[{limits['min_temp_c']}, {limits['max_temp_c']}]°C",
                           zone_id=zone, details={"temperature_c": temp, "limits": [limits["min_temp_c"],
                                                                                    limits["max_temp_c"]]})


def _flush_loop():
    db = firestore_client()
    while not _stop.wait(FLUSH_S):
        with _lock:
            batch_items = [(z, dict(_state[z])) for z in _dirty]
            _dirty.clear()
        if not batch_items:
            continue
        try:
            batch = db.batch()
            for zone, s in batch_items:
                batch.set(db.collection("zone_state").document(zone), s)
                if "temperature_c" in s:
                    ZONE_TEMP.labels(zone).set(s["temperature_c"])
                ZONE_OCC.labels(zone).set(s["occupancy_estimate"])
            batch.commit()
            ZONES_TRACKED.set(len(_state))
        except Exception:  # noqa: BLE001
            log.exception("firestore flush failed")
            with _lock:
                _dirty.update(z for z, _ in batch_items)


@asynccontextmanager
async def lifespan(_app):
    global alerts, consumer
    alerts = AlertPublisher()
    consumer = JsonConsumerLoop([TOPIC_SENSOR_READINGS], os.environ.get("CONSUMER_GROUP", "occupancy-service"),
                                handle_reading)
    consumer.start()
    threading.Thread(target=_flush_loop, name="firestore-flush", daemon=True).start()
    yield
    _stop.set()
    consumer.stop()


app = create_app("Occupancy Service", "/api/occupancy", lifespan=lifespan)


@app.get("/api/occupancy")
def all_zones():
    """Current state of every zone (from Firestore, so it is consistent across replicas)."""
    return [d.to_dict() for d in firestore_client().collection("zone_state").stream()]


@app.get("/api/occupancy/{zone_id}")
def zone_state(zone_id: str):
    doc = firestore_client().collection("zone_state").document(zone_id).get()
    if not doc.exists:
        raise HTTPException(404, "no data for zone yet")
    return doc.to_dict()
