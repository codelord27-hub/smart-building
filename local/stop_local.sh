#!/usr/bin/env bash
RUN_DIR="${RUN_DIR:-/tmp/sb-local}"
for f in "$RUN_DIR"/*.pid; do
  [ -e "$f" ] || continue
  kill "$(cat "$f")" 2>/dev/null && echo "stopped $(basename "$f" .pid)"
  rm -f "$f"
done
pkill -f "com.smartbuilding.analytics.LocalSocketJob" 2>/dev/null || true
pkill -f "local/flink_bridge.py|flink_bridge.py|pubsub_to_function.py|gcp-storage-emulator|functions-framework --target handle_alert" 2>/dev/null || true
pkill -f "uvicorn (app|gateway):app --host 127.0.0.1" 2>/dev/null || true
