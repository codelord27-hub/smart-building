#!/usr/bin/env bash
# Runs the complete system on one machine for development/testing, with local stand-ins for
# the managed cloud services (see local/README.md). Logs go to $RUN_DIR/logs.
#   Kafka      -> any Kafka-compatible broker on 127.0.0.1:9092 (Tansu / Kafka binary)
#   Pub/Sub    -> Pub/Sub emulator on 127.0.0.1:8085
#   Firestore  -> Firestore emulator on 127.0.0.1:8080
#   GCS        -> gcp-storage-emulator on 127.0.0.1:9023
#   Cloud SQL  -> local Postgres with databases registry, energy, hvac, optimizer
#   Dataproc   -> Flink 1.17 local mini-cluster (FLINK_LIB dir) fed by local/flink_bridge.py
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_DIR="${RUN_DIR:-/tmp/sb-local}"
FLINK_LIB="${FLINK_LIB:-/opt/flink-1.17/lib}"
FLINK_CLASSES="${FLINK_CLASSES:-/tmp/flinkjob/classes}"
mkdir -p "$RUN_DIR/logs"

source "$ROOT/local/env.sh"
export CONTROL_INTERVAL_SECONDS="${CONTROL_INTERVAL_SECONDS:-15}"

start() {  # name, workdir, command...
  local name=$1 dir=$2; shift 2
  (cd "$dir" && SERVICE_NAME=$name nohup "$@" > "$RUN_DIR/logs/$name.log" 2>&1 & echo $! > "$RUN_DIR/$name.pid")
  echo "started $name (pid $(cat "$RUN_DIR/$name.pid"))"
}
svc() {  # name, port, [extra env...]
  local name=$1 port=$2; shift 2
  start "$name" "$ROOT/services/$name" env "$@" uvicorn app:app --host 127.0.0.1 --port "$port" --no-access-log
}

start gcs-emulator "$RUN_DIR" gcp-storage-emulator start --host 127.0.0.1 --port 9023 --default-bucket "$RAW_BUCKET" --in-memory
start pubsub-bridge "$ROOT/local" python pubsub_to_function.py
start alert-service "$ROOT/functions/alert-service" functions-framework --target handle_alert --signature-type cloudevent --port 8090
sleep 3
svc building-registry 8001 DB_NAME=registry
svc hvac-controller 8006 DB_NAME=hvac GRPC_PORT=50051
svc energy-monitor 8004 DB_NAME=energy GRPC_PORT=50052 ENERGY_TICK_SECONDS=5
svc sensor-ingestion 8002 ARCHIVE_INTERVAL_SECONDS=10
svc occupancy-service 8003
svc control-optimizer 8005 DB_NAME=optimizer
start gateway "$ROOT/local" uvicorn gateway:app --host 127.0.0.1 --port 8000 --no-access-log
start flink-bridge "$ROOT/local" env FLINK_CLASSPATH="$FLINK_CLASSES:$(ls "$FLINK_LIB"/*.jar | tr '\n' ':')" \
  JAVA_OPTS="$(for p in java.lang java.lang.reflect java.lang.invoke java.io java.net java.nio sun.nio.ch java.text \
    java.time java.util java.util.concurrent java.util.concurrent.atomic java.util.concurrent.locks java.math \
    java.security sun.security.action; do printf -- '--add-opens java.base/%s=ALL-UNNAMED ' "$p"; done)" \
  WINDOW_SECONDS="${WINDOW_SECONDS:-60}" python flink_bridge.py
sleep 4
svc building-simulator 8007 SIM_INTERVAL_SECONDS=3
echo "dashboard: http://127.0.0.1:8000/   logs: $RUN_DIR/logs"
