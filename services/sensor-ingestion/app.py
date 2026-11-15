"""Sensor Ingestion Service - receives temperature and occupancy readings.

* POST /api/readings         - JSON batch from sensors/gateways -> validated -> Kafka `sensor-readings`
* POST /api/readings/upload  - bulk file (NDJSON or CSV) -> raw file stored in Cloud Storage -> Kafka
* Every ARCHIVE_INTERVAL_SECONDS the raw readings are archived as NDJSON to Cloud Storage
  (data-lake style, partitioned by date/hour).
Stateless -> scaled horizontally by an HPA on CPU.
"""
import csv
import io
import json
import logging
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import File, HTTPException, UploadFile
from prometheus_client import Counter
from pydantic import BaseModel, Field, ValidationError

from sbcommon.config import POD_NAME, TOPIC_SENSOR_READINGS, env, env_float, env_int
from sbcommon.gcp import storage_client
from sbcommon.kafka import JsonProducer
from sbcommon.logs import setup_logging
from sbcommon.web import create_app

log = setup_logging()

RAW_BUCKET = env("RAW_BUCKET", "sb-raw-local")
ARCHIVE_S = env_float("ARCHIVE_INTERVAL_SECONDS", 30)
ARCHIVE_MAX = env_int("ARCHIVE_MAX_RECORDS", 5000)
MAX_UPLOAD_BYTES = env_int("MAX_UPLOAD_BYTES", 10 * 1024 * 1024)

READINGS = Counter("sensor_readings_ingested_total", "Sensor readings accepted", ["source"])
REJECTED = Counter("sensor_readings_rejected_total", "Sensor readings rejected by validation", ["source"])
ARCHIVED = Counter("raw_archive_objects_total", "Raw NDJSON objects written to Cloud Storage", ["result"])

producer = None
_buffer: list[dict] = []
_buf_lock = threading.Lock()
_stop = threading.Event()


class ReadingIn(BaseModel):
    sensor_id: str = Field(min_length=1, max_length=80)
    zone_id: str = Field(pattern=r"^[a-z0-9-]{3,60}$")
    building_id: str | None = Field(default=None, max_length=60)
    temperature_c: float | None = Field(default=None, ge=-40, le=80)
    occupancy_count: int | None = Field(default=None, ge=0, le=10000)
    humidity_pct: float | None = Field(default=None, ge=0, le=100)
    ts_ms: int | None = Field(default=None, ge=0)


class ReadingBatch(BaseModel):
    readings: list[ReadingIn] = Field(min_length=1, max_length=1000)


def _publish(readings: list[ReadingIn], source: str) -> int:
    now_ms = int(time.time() * 1000)
    records = []
    for r in readings:
        if r.temperature_c is None and r.occupancy_count is None:
            REJECTED.labels(source).inc()
            continue
        rec = r.model_dump()
        rec["ts_ms"] = rec["ts_ms"] or now_ms
        rec["reading_id"] = uuid.uuid4().hex
        rec["event_type"] = "sensor_reading"
        rec["ingested_by"] = POD_NAME
        producer.send(TOPIC_SENSOR_READINGS, rec, key=rec["zone_id"])
        records.append(rec)
    with _buf_lock:
        _buffer.extend(records)
        flush_now = len(_buffer) >= ARCHIVE_MAX
    if flush_now:
        threading.Thread(target=_archive_once, daemon=True).start()
    READINGS.labels(source).inc(len(records))
    return len(records)


def _archive_once():
    with _buf_lock:
        items = _buffer[:]
        _buffer.clear()
    if not items:
        return
    now = datetime.now(timezone.utc)
    name = f"raw/readings/dt={now:%Y-%m-%d}/hour={now:%H}/{POD_NAME}-{now:%H%M%S}-{uuid.uuid4().hex[:8]}.ndjson"
    body = "\n".join(json.dumps(i, separators=(",", ":")) for i in items) + "\n"
    try:
        storage_client().bucket(RAW_BUCKET).blob(name).upload_from_string(body, content_type="application/x-ndjson")
        ARCHIVED.labels("ok").inc()
        log.info("raw readings archived", extra={"object": name, "records": len(items)})
    except Exception as exc:  # noqa: BLE001
        ARCHIVED.labels("error").inc()
        log.error("raw archive failed", extra={"error": str(exc), "records": len(items)})


def _archive_loop():
    while not _stop.wait(ARCHIVE_S):
        _archive_once()


@asynccontextmanager
async def lifespan(_app):
    global producer
    producer = JsonProducer()
    threading.Thread(target=_archive_loop, name="raw-archiver", daemon=True).start()
    yield
    _stop.set()
    _archive_once()
    producer.close()


app = create_app("Sensor Ingestion Service", "/api/readings", lifespan=lifespan)


@app.post("/api/readings", status_code=202)
def ingest(batch: ReadingBatch):
    accepted = _publish(batch.readings, "api")
    return {"accepted": accepted, "rejected": len(batch.readings) - accepted}


def _parse_upload(raw: bytes, filename: str) -> tuple[list[ReadingIn], int]:
    text = raw.decode("utf-8-sig")
    rows: list[dict] = []
    if filename.lower().endswith(".csv"):
        for row in csv.DictReader(io.StringIO(text)):
            rows.append({k.strip(): (v.strip() if isinstance(v, str) and v.strip() != "" else None)
                         for k, v in row.items() if k})
    else:
        rows = [json.loads(line) for line in text.splitlines() if line.strip()]
    ok, bad = [], 0
    for row in rows:
        try:
            ok.append(ReadingIn.model_validate(row))
        except ValidationError:
            bad += 1
    return ok, bad


@app.post("/api/readings/upload", status_code=202)
async def upload(file: UploadFile = File(...)):
    raw = await file.read()
    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, "file too large")
    name = file.filename or "upload.ndjson"
    if not name.lower().endswith((".csv", ".ndjson", ".jsonl", ".json")):
        raise HTTPException(415, "upload .csv or .ndjson files")
    try:
        readings, bad = _parse_upload(raw, name)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(400, f"could not parse file: {exc}") from exc
    now = datetime.now(timezone.utc)
    obj = f"uploads/dt={now:%Y-%m-%d}/{now:%H%M%S}-{uuid.uuid4().hex[:8]}-{name}"
    storage_client().bucket(RAW_BUCKET).blob(obj).upload_from_string(raw, content_type=file.content_type
                                                                        or "application/octet-stream")
    REJECTED.labels("upload").inc(bad)
    accepted = _publish(readings, "upload") if readings else 0
    log.info("bulk upload processed", extra={"object": obj, "accepted": accepted, "rejected": bad})
    return {"object": f"gs://{RAW_BUCKET}/{obj}", "accepted": accepted, "rejected": bad + len(readings) - accepted}
