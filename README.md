# S21 · Smart Building Management: HVAC and energy control (concept: Two-Phase Commit)

Cloud Computing (CS/SS G527) assignment, deployed on **Google Cloud**.

## The 8 microservices

| # | Service | Runs on | Talks via | Data |
|---|---|---|---|---|
| 1 | Building Registry (public dashboard at `/`) | GKE | REST | Cloud SQL `registry` |
| 2 | Sensor Ingestion (**HPA**) | GKE | REST in → Kafka `sensor-readings` | Cloud Storage (raw archive and uploads) |
| 3 | Occupancy Service | GKE | Kafka consumer, REST, Pub/Sub alerts | Firestore `zone_state` |
| 4 | Energy Monitor (2PC participant) | GKE | **gRPC**, Kafka (in/out), REST | Cloud SQL `energy`, Firestore `energy_analytics` |
| 5 | Control Optimizer (2PC coordinator, **HPA**) | GKE | REST, **gRPC** client | Cloud SQL `optimizer` (2PC log) |
| 6 | HVAC Controller (2PC participant) | GKE | **gRPC**, REST, Kafka out | Cloud SQL `hvac` |
| 7 | Alert Service | **Cloud Function (serverless)** | Pub/Sub trigger (async) | Firestore `alerts` |
| 8 | Energy Analytics | **Flink on Dataproc** | Kafka → 1-min windows → Kafka `energy-analytics-results` | Flink state |

`building-simulator` is a test harness (simulated physics and sensors), not one of the 8 services.

## Repository layout

```
services/            Python microservices (FastAPI + gRPC), shared lib in services/common, one Dockerfile
proto/               gRPC contract of the Two-Phase Commit protocol
functions/           Alert Service (Cloud Functions 2nd gen)
analytics/           Flink job (Java 11, Flink 1.17 = Dataproc 2.2)
infra/terraform/     IaC: 1-bootstrap (state bucket), 2-infra (all GCP resources), 3-platform (cluster bootstrap + ArgoCD)
k8s/                 GitOps manifests tracked by ArgoCD: argocd/apps (app-of-apps), apps (services, HPA, Ingress,
                     ServiceMonitor, Grafana dashboard), platform/kafka (Strimzi Kafka + topics)
.github/workflows/   CI (tests), image build + GitOps tag bump, Flink deploy
loadtest/            k6 script for the HPA demonstration
local/               run the whole system on one machine with emulators (development/testing)
tests/               unit tests + end-to-end 2PC scenario test
docs/                RUNBOOK.md (step-by-step deployment and demo guide)
```

Deployment: **[docs/RUNBOOK.md](docs/RUNBOOK.md)**.

## Testing

```bash
pytest tests/unit -q                                   # Python unit tests
mvn -f analytics/energy-analytics/pom.xml package      # Flink unit tests + fat jar
./local/run_local.sh && python tests/e2e_2pc_local.py  # full system locally + 26 2PC end-to-end checks
```
