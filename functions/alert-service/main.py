"""Alert Service - Cloud Function (2nd gen), triggered asynchronously by Pub/Sub.

Every message published to the `building-alerts` topic (by the Occupancy Service, Control
Optimizer or Energy Monitor) invokes this function via Eventarc. It
  1. validates and normalises the alert,
  2. stores it idempotently in Firestore `alerts/{pubsub message id}` (Pub/Sub is at-least-once),
  3. keeps per-type counters in `alert_stats/{type}`,
  4. writes a structured log entry whose severity maps to Cloud Logging severity
     (so log-based alerting/notification can be attached).
"""
import base64
import json
import os
import time

import functions_framework
from google.cloud import firestore

_db = None
SEVERITIES = {"INFO", "WARNING", "ERROR", "CRITICAL"}


def db() -> firestore.Client:
    global _db
    if _db is None:
        _db = firestore.Client(project=os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT"),
                               database=os.environ.get("FIRESTORE_DATABASE", "(default)"))
    return _db


def log(severity: str, message: str, **fields):
    print(json.dumps({"severity": severity, "message": message, "service": "alert-service", **fields},
                     default=str), flush=True)


def normalise(raw: dict, message_id: str, attributes: dict) -> dict:
    severity = str(raw.get("severity") or attributes.get("severity") or "WARNING").upper()
    if severity not in SEVERITIES:
        severity = "WARNING"
    alert_type = str(raw.get("type") or attributes.get("type") or "UNKNOWN").upper()[:64]
    return {
        "type": alert_type,
        "severity": severity,
        "message": str(raw.get("message", ""))[:1000],
        "zone_id": raw.get("zone_id"),
        "source": raw.get("source") or attributes.get("source"),
        "ts_ms": int(raw.get("ts_ms") or time.time() * 1000),
        "details": raw.get("details") if isinstance(raw.get("details"), dict) else {},
        "message_id": message_id,
    }


@functions_framework.cloud_event
def handle_alert(cloud_event):
    msg = (cloud_event.data or {}).get("message", {})
    message_id = msg.get("messageId") or msg.get("message_id") or cloud_event["id"]
    attributes = msg.get("attributes") or {}
    try:
        raw = json.loads(base64.b64decode(msg.get("data", "")).decode("utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("alert payload must be a JSON object")
    except Exception as exc:  # noqa: BLE001  - bad message: log and ack (do not retry forever)
        log("ERROR", "discarding malformed alert", error=str(exc), message_id=message_id)
        return

    alert = normalise(raw, message_id, attributes)
    client = db()
    ref = client.collection("alerts").document(message_id)
    if ref.get().exists:  # redelivery of the same Pub/Sub message -> idempotent no-op
        log("INFO", "duplicate alert ignored", message_id=message_id)
        return
    ref.set({**alert, "received_at": firestore.SERVER_TIMESTAMP})
    client.collection("alert_stats").document(alert["type"]).set(
        {"type": alert["type"], "count": firestore.Increment(1), "last_ts_ms": alert["ts_ms"]}, merge=True)

    log(alert["severity"] if alert["severity"] != "CRITICAL" else "CRITICAL",
        f"ALERT {alert['type']} zone={alert['zone_id']}: {alert['message']}",
        alert=alert, labels={"alert_type": alert["type"]})
