# Runbook: deploy the Smart Building platform on Google Cloud

Everything runs from **Google Cloud Shell** (it already has `gcloud`, `terraform`, `kubectl`, `docker` and `git`).
Plan for about 1.5 hours the first time; most of it is waiting for Google to create resources.

Replace these placeholders throughout:

| Placeholder | Example |
|---|---|
| `PROJECT_ID` | `sb-cc-assignment-2026` |
| `GH_USER` | your GitHub user name |
| `REPO_URL` | `https://github.com/GH_USER/smart-building.git` |

---

## 0. One-time prerequisites (about 10 min)

1. **Google Cloud account** with billing: https://console.cloud.google.com → activate the free trial ($300 credit).
   * The free trial allows at most **8 vCPUs running at once**. The default sizes here use exactly 8
     (1 GKE node + 1 Dataproc node, each with 4 vCPUs). If you click **"Activate full account"** (the credit still applies) you get
     more quota and can use `sizing-paid.tfvars.example` for a roomier cluster.
2. **Create a project**: console → project picker → *New project* → note the **Project ID**.
3. **GitHub**: create an **empty public repository** called `smart-building` (no README, no .gitignore).
4. **GitHub token** for pushing from Cloud Shell: GitHub → Settings → Developer settings → Personal access tokens →
   *Tokens (classic)* → scopes **`repo`** and **`workflow`** (`workflow` is required because the repo contains
   GitHub Actions files). Copy the token; you use it as the password when `git push` asks.

## 1. Get the code into Cloud Shell (2 min)

Open https://shell.cloud.google.com, then **⋮ → Upload** `smart-building.zip`.

```bash
unzip -q smart-building.zip -d ~ && cd ~/smart-building
export PROJECT_ID=PROJECT_ID
export REPO_URL=https://github.com/GH_USER/smart-building.git
gcloud config set project $PROJECT_ID
```

## 2. Terraform stage 1: state bucket (1 min)

```bash
cd ~/smart-building/infra/terraform/1-bootstrap
terraform init
terraform apply -var project_id=$PROJECT_ID -auto-approve
```

## 3. Terraform stage 2: all cloud infrastructure (about 25-30 min)

Creates the VPC, GKE, Cloud SQL, Firestore, GCS buckets, Pub/Sub, the Alert Service Cloud Function,
the Dataproc Flink cluster, Artifact Registry, IAM and the GitHub Workload Identity Federation.

```bash
cd ~/smart-building/infra/terraform/2-infra
cat > terraform.tfvars <<EOF
project_id  = "$PROJECT_ID"
github_repo = "GH_USER/smart-building"
EOF
terraform init -backend-config="bucket=${PROJECT_ID}-tfstate"
terraform apply            # review, type "yes"
```

> If it stops with an error such as *"API has not been used in project … or it is disabled"* or *"Permission denied … service account"*,
> wait 1-2 minutes and run `terraform apply` again: new APIs and IAM bindings take a moment to propagate. Terraform continues where it stopped.

## 4. Point the repository at your project and push (3 min)

```bash
cd ~/smart-building
./scripts/configure-repo.sh $REPO_URL
git init -b main
git add -A && git commit -m "Smart Building platform"
git remote add origin $REPO_URL
git push -u origin main          # username = GH_USER, password = the token from step 0
```

On GitHub → **Actions** you will see:
* `ci`: unit tests (Python + Flink).
* `build-services`: builds and pushes the 7 images, then commits the new image tags to `k8s/apps/kustomization.yaml` (about 6-8 min).
* `deploy-flink`: builds the Flink jar and submits it to Dataproc.

## 5. Terraform stage 3: cluster bootstrap and Argo CD (about 5 min)

```bash
cd ~/smart-building/infra/terraform/3-platform
terraform init -backend-config="bucket=${PROJECT_ID}-tfstate"
terraform apply -var project_id=$PROJECT_ID -var state_bucket=${PROJECT_ID}-tfstate -var git_repo_url=$REPO_URL
```

From here **Argo CD deploys everything from Git** (Strimzi + Kafka, Prometheus/Grafana, all services).
Nothing is deployed with `kubectl apply`.

```bash
gcloud container clusters get-credentials sb-gke --zone asia-south1-a
kubectl get applications -n argocd          # wait until all are Synced / Healthy (about 10 min)
kubectl get pods -A | grep -v kube-system
```

## 6. Start / restart the Flink job (2 min)

Kafka must be running first (`kubectl get kafka -n kafka` shows READY True). Then:
GitHub → Actions → **deploy-flink** → *Run workflow*. Check it in the console: **Dataproc → Jobs**
(status *Running*). The dashboard's *"Last 1-min window (Flink)"* column fills after about 2 minutes.

## 7. Open everything

```bash
kubectl get ingress -n smart-building                # ADDRESS = public URL (takes 5-10 min the first time)
kubectl get svc -n argocd argocd-server              # EXTERNAL-IP -> Argo CD UI (http)
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d; echo
kubectl get svc -n monitoring monitoring-grafana     # EXTERNAL-IP -> Grafana (user: admin)
terraform -chdir=infra/terraform/3-platform output -raw grafana_admin_password; echo
```

* Dashboard: `http://INGRESS_IP/`, with Swagger docs at `/api/registry/docs`, `/api/readings/docs`, `/api/control/docs`, …
* Grafana → Dashboards → **"Smart Building - Services, HPA & Cluster Health"**

## 8. Demo script (what to show, by requirement)

| Req | What to show |
|---|---|
| a. IaC | `infra/terraform/*`; `terraform state list` in 2-infra |
| b. Serverless + communication | Cloud Functions → `alert-service` (Pub/Sub trigger); REST (Swagger pages), gRPC (`proto/sbproto/twopc.proto`), Kafka topics (`kubectl get kafkatopics -n kafka`), Pub/Sub topic `building-alerts` |
| c. GKE + HPA + load test | `kubectl get hpa -n smart-building -w` in one tab; in another: `docker run --rm -i -e BASE_URL=http://INGRESS_IP grafana/k6 run - < loadtest/hpa-load.js`. Replicas go 1 → 4-6 and back. Grafana shows RPS, latency and replica panels |
| d. GitOps | Argo CD UI; change e.g. `replicas` of `building-registry` in Git → push → Argo CD syncs. CI only commits image tags |
| e. Flink | Dataproc → Jobs → running job; dashboard column *Last 1-min window*; `energy-analytics-results` topic |
| f. **2PC** | Dashboard → *Operator command*: (1) COOL 60 % → COMMITTED; (2) set budget to 10 kW → command → ABORTED (energy-monitor votes NO), alert appears; (3) set an HVAC unit to MAINTENANCE → ABORTED (hvac-controller votes NO); (4) choose *crash coordinator after COMMIT decision* → the pod restarts and recovery completes the transaction as COMMITTED (`recovered = yes`); (5) *crash before decision* → recovered as ABORTED |
| g. Storage | Cloud SQL (4 databases), Firestore (`zone_state`, `energy_analytics`, `alerts`), GCS `PROJECT_ID-sb-raw` (`raw/readings/...`, `uploads/...`) |
| h. Observability | Grafana dashboard; Cloud Logging queries below |

**Cloud Logging** (console → Logging → Logs Explorer):

```
resource.type="k8s_container" AND resource.labels.namespace_name="smart-building"      # all microservices
jsonPayload.logger="twopc"                                                             # every 2PC transaction
resource.type="cloud_run_revision" AND resource.labels.service_name="alert-service"    # serverless function
resource.type="cloud_dataproc_job" OR resource.type="cloud_dataproc_cluster"           # Flink analytics job
severity>=ERROR                                                                        # errors across everything
```

Extra demo controls: force an abnormal temperature with
`curl -X POST http://INGRESS_IP/api/sim/zones/f0-server-room/spike -H 'content-type: application/json' -d '{"temperature_c":35}'`.
Upload a bulk file with `curl -F file=@readings.csv http://INGRESS_IP/api/readings/upload`.

## 9. Cost and teardown

Running cost is about **$12/day** (GKE node, Dataproc node, Cloud SQL, load balancers), well inside the $300 credit for the
remaining days. After the demo:

```bash
cd ~/smart-building && ./scripts/teardown.sh $PROJECT_ID $REPO_URL
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Quota 'CPUS_ALL_REGIONS' exceeded` | Free-trial limit: keep the default sizes, or activate the full account |
| Pods `ImagePullBackOff` | `build-services` has not finished yet; wait, Argo CD retries automatically |
| Argo CD app `kafka-cluster` *Unknown / missing CRD* | Strimzi operator still installing; it retries every 20 s |
| Ingress has no ADDRESS after 15 min | `kubectl describe ingress -n smart-building smart-building` (events show health-check state) |
| Flink job FAILED with Kafka timeout | Kafka was not ready yet; re-run the `deploy-flink` workflow |
| `terraform apply` Cloud Function build error | wait 2 min and re-apply (IAM propagation) |
