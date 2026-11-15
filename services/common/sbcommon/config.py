import os
import socket


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable {name}")
    return value


def env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


def env_bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


SERVICE_NAME = os.environ.get("SERVICE_NAME", "unknown-service")
POD_NAME = os.environ.get("POD_NAME", socket.gethostname())
GCP_PROJECT = os.environ.get("GCP_PROJECT", os.environ.get("GOOGLE_CLOUD_PROJECT", "demo-sb"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

# Kafka topics (created by Strimzi KafkaTopic resources in k8s/platform/kafka)
TOPIC_SENSOR_READINGS = os.environ.get("TOPIC_SENSOR_READINGS", "sensor-readings")
TOPIC_ENERGY_EVENTS = os.environ.get("TOPIC_ENERGY_EVENTS", "energy-events")
TOPIC_ANALYTICS_RESULTS = os.environ.get("TOPIC_ANALYTICS_RESULTS", "energy-analytics-results")

# Pub/Sub topic consumed by the Alert Service (Cloud Function)
PUBSUB_ALERTS_TOPIC = os.environ.get("PUBSUB_ALERTS_TOPIC", "building-alerts")
