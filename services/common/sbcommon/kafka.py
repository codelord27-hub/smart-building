"""Kafka producer / consumer helpers built on confluent-kafka (librdkafka)."""
import json
import logging
import threading
from typing import Callable

from confluent_kafka import Consumer, KafkaError, Producer
from prometheus_client import Counter

from .config import KAFKA_BOOTSTRAP, SERVICE_NAME

log = logging.getLogger("kafka")

KAFKA_PRODUCED = Counter("kafka_messages_produced_total", "Kafka messages produced", ["service", "topic", "result"])
KAFKA_CONSUMED = Counter("kafka_messages_consumed_total", "Kafka messages consumed", ["service", "topic", "result"])


class JsonProducer:
    def __init__(self, bootstrap: str = KAFKA_BOOTSTRAP):
        self._producer = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": SERVICE_NAME,
            "acks": "all",
            "linger.ms": 10,
            "message.timeout.ms": 30000,
            "socket.keepalive.enable": True,
        })
        self._stop = threading.Event()
        self._poller = threading.Thread(target=self._poll_loop, name="kafka-producer-poll", daemon=True)
        self._poller.start()

    def _poll_loop(self):
        while not self._stop.is_set():
            self._producer.poll(0.5)

    def send(self, topic: str, value: dict, key: str | None = None) -> None:
        def _cb(err, _msg):
            if err is not None:
                KAFKA_PRODUCED.labels(SERVICE_NAME, topic, "error").inc()
                log.error("kafka delivery failed", extra={"topic": topic, "error": str(err)})
            else:
                KAFKA_PRODUCED.labels(SERVICE_NAME, topic, "ok").inc()

        payload = json.dumps(value, separators=(",", ":")).encode()
        while True:
            try:
                self._producer.produce(topic, payload, key=key.encode() if key else None, on_delivery=_cb)
                return
            except BufferError:
                # local queue full under heavy load -> let librdkafka drain, then retry
                self._producer.poll(0.1)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)

    def close(self):
        self.flush()
        self._stop.set()


class JsonConsumerLoop:
    """Runs a consumer in a background thread and calls handler(topic, key, value_dict) per message.
    Offsets are committed automatically (at-least-once)."""

    def __init__(self, topics: list[str], group_id: str, handler: Callable[[str, str | None, dict], None],
                 bootstrap: str = KAFKA_BOOTSTRAP, offset_reset: str = "latest"):
        self.topics = topics
        self.handler = handler
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "client.id": f"{SERVICE_NAME}-consumer",
            "auto.offset.reset": offset_reset,
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 2000,
            "session.timeout.ms": 30000,
        })
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name=f"kafka-consumer-{group_id}", daemon=True)
        self.healthy = True

    def start(self):
        self._consumer.subscribe(self.topics)
        self._thread.start()
        log.info("kafka consumer started", extra={"topics": self.topics})

    def _run(self):
        while not self._stop.is_set():
            try:
                msg = self._consumer.poll(1.0)
            except Exception as exc:  # noqa: BLE001
                log.error("kafka poll failed", extra={"error": str(exc)})
                continue
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("kafka consumer error", extra={"error": str(msg.error())})
                continue
            topic = msg.topic()
            try:
                value = json.loads(msg.value())
                key = msg.key().decode() if msg.key() else None
                self.handler(topic, key, value)
                KAFKA_CONSUMED.labels(SERVICE_NAME, topic, "ok").inc()
            except Exception as exc:  # noqa: BLE001  (poison message must not kill the loop)
                KAFKA_CONSUMED.labels(SERVICE_NAME, topic, "error").inc()
                log.exception("failed to handle kafka message", extra={"topic": topic, "error": str(exc)})

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=5)
        try:
            self._consumer.close()
        except Exception:  # noqa: BLE001
            pass
