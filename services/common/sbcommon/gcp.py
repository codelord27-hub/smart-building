"""Google Cloud clients: Pub/Sub (alerts), Firestore (NoSQL state) and Cloud Storage (raw data).

On GKE the clients authenticate through Workload Identity. Locally they talk to emulators
when PUBSUB_EMULATOR_HOST / FIRESTORE_EMULATOR_HOST / STORAGE_EMULATOR_HOST are set.
"""
import json
import logging
import os
import threading
import time
from functools import lru_cache

from prometheus_client import Counter

from .config import GCP_PROJECT, PUBSUB_ALERTS_TOPIC, SERVICE_NAME

log = logging.getLogger("gcp")

ALERTS_PUBLISHED = Counter("alerts_published_total", "Alerts published to Pub/Sub", ["service", "type", "result"])


@lru_cache(maxsize=1)
def firestore_client():
    from google.cloud import firestore

    database = os.environ.get("FIRESTORE_DATABASE", "(default)")
    return firestore.Client(project=GCP_PROJECT, database=database)


@lru_cache(maxsize=1)
def storage_client():
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import storage

    if os.environ.get("STORAGE_EMULATOR_HOST"):
        return storage.Client(project=GCP_PROJECT, credentials=AnonymousCredentials())
    return storage.Client(project=GCP_PROJECT)


class AlertPublisher:
    """Publishes alert events to Pub/Sub. The Alert Service (Cloud Function) is triggered
    asynchronously by every message. A per-key cooldown avoids flooding during incidents."""

    def __init__(self, topic: str = PUBSUB_ALERTS_TOPIC, cooldown_s: float = 60.0):
        from google.cloud import pubsub_v1

        self._client = pubsub_v1.PublisherClient()
        self._topic_path = self._client.topic_path(GCP_PROJECT, topic)
        self._cooldown_s = cooldown_s
        self._last: dict[str, float] = {}
        self._lock = threading.Lock()

    def publish(self, alert_type: str, severity: str, message: str, zone_id: str | None = None,
                details: dict | None = None, dedupe_key: str | None = None) -> bool:
        key = dedupe_key or f"{alert_type}:{zone_id}"
        now = time.time()
        with self._lock:
            if now - self._last.get(key, 0) < self._cooldown_s:
                return False
            self._last[key] = now
        event = {
            "type": alert_type,
            "severity": severity,
            "message": message,
            "zone_id": zone_id,
            "source": SERVICE_NAME,
            "ts_ms": int(now * 1000),
            "details": details or {},
        }
        try:
            future = self._client.publish(self._topic_path, json.dumps(event).encode(),
                                          type=alert_type, severity=severity, source=SERVICE_NAME)
            future.add_done_callback(lambda f: self._done(f, alert_type))
            log.warning("alert published", extra={"alert_type": alert_type, "zone_id": zone_id,
                                                   "alert_severity": severity, "alert_message": message})
            return True
        except Exception as exc:  # noqa: BLE001
            ALERTS_PUBLISHED.labels(SERVICE_NAME, alert_type, "error").inc()
            log.error("alert publish failed", extra={"alert_type": alert_type, "error": str(exc)})
            return False

    @staticmethod
    def _done(future, alert_type):
        try:
            future.result()
            ALERTS_PUBLISHED.labels(SERVICE_NAME, alert_type, "ok").inc()
        except Exception as exc:  # noqa: BLE001
            ALERTS_PUBLISHED.labels(SERVICE_NAME, alert_type, "error").inc()
            log.error("alert publish failed", extra={"alert_type": alert_type, "error": str(exc)})
