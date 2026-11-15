"""Local stand-in for the Flink Kafka connector (Maven Central is not reachable in the build sandbox).

Runs the real Flink 1.17 job logic (LocalSocketJob -> AnalyticsPipeline, same code as the
production EnergyAnalyticsJob) in a local mini-cluster:
  Kafka (sensor-readings, energy-events) --> TCP socket --> Flink --> stdout "RESULT {...}" --> Kafka results topic
"""
import json
import os
import socket
import subprocess
import sys
import threading

from confluent_kafka import OFFSET_END, Consumer, Producer, TopicPartition

BOOT = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
PORT = int(os.environ.get("BRIDGE_PORT", "9999"))
WINDOW = os.environ.get("WINDOW_SECONDS", "60")
CLASSPATH = os.environ["FLINK_CLASSPATH"]

srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", PORT))
srv.listen(1)

# Flink 1.17 targets Java 11 (as on Dataproc); on newer local JDKs it needs these module opens.
JAVA_OPTS = os.environ.get("JAVA_OPTS", "").split()
flink = subprocess.Popen(["java", *JAVA_OPTS, "-Xmx1g", "-cp", CLASSPATH, "com.smartbuilding.analytics.LocalSocketJob",
                          "--port", str(PORT), "--window-seconds", WINDOW],
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
conn, _ = srv.accept()
print("flink connected to bridge", flush=True)

producer = Producer({"bootstrap.servers": BOOT})


def pump_results():
    for line in flink.stdout:
        if line.startswith("RESULT "):
            payload = line[len("RESULT "):].strip()
            zone = json.loads(payload)["zone_id"]
            producer.produce("energy-analytics-results", payload.encode(), key=zone.encode())
            producer.poll(0)
            print("window result ->", payload[:160], flush=True)
        elif "Exception" in line or "ERROR" in line:
            print("flink:", line.rstrip(), flush=True)
    print("flink exited", flink.wait(), flush=True)
    os._exit(1)


threading.Thread(target=pump_results, daemon=True).start()
# Like Flink's KafkaSource: explicit partition assignment (no consumer-group membership), start at the end.
consumer = Consumer({"bootstrap.servers": BOOT, "group.id": "energy-analytics-flink-local",
                     "enable.auto.commit": False})
meta = consumer.list_topics(timeout=10)
consumer.assign([TopicPartition(t, p, OFFSET_END) for t in ("sensor-readings", "energy-events")
                 for p in meta.topics[t].partitions])
while True:
    m = consumer.poll(1.0)
    if m is None or m.error():
        continue
    try:
        conn.sendall(m.value().replace(b"\n", b" ") + b"\n")
    except OSError:
        sys.exit("flink socket closed")
