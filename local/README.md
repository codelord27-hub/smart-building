# Running everything locally (development / testing)

`run_local.sh` starts all services on one machine, with local stand-ins for the managed services:

| Cloud service | Local stand-in |
|---|---|
| Kafka (Strimzi on GKE) | any Kafka-compatible broker on `127.0.0.1:9092` (Apache Kafka, Tansu, Redpanda) with topics `sensor-readings`, `energy-events`, `energy-analytics-results` (3 partitions) |
| Pub/Sub | Pub/Sub emulator on `127.0.0.1:8085` (`gcloud beta emulators pubsub start`) |
| Firestore | Firestore emulator on `127.0.0.1:8080` (`gcloud emulators firestore start`) |
| Cloud Storage | `gcp-storage-emulator` (pip) on `127.0.0.1:9023` |
| Cloud SQL | local PostgreSQL with user `sbapp`/`localpass` and databases `registry`, `energy`, `hvac`, `optimizer` |
| Eventarc → Cloud Function | `pubsub_to_function.py` + `functions-framework` |
| Dataproc Flink + Kafka connector | `flink_bridge.py` + Flink 1.17 libraries (`pip download apache-flink-libraries==1.17.0`) running `LocalSocketJob` (same pipeline code as the production job) |
| GKE Ingress | `gateway.py` (same path routing as `k8s/apps/ingress.yaml`) |

```bash
pip install -r services/requirements.txt functions-framework gcp-storage-emulator
./scripts/gen_proto.sh && pip install -e services/common
./local/run_local.sh            # dashboard on http://127.0.0.1:8000
python tests/e2e_2pc_local.py   # 26 end-to-end Two-Phase Commit checks
./local/stop_local.sh
```
