#!/usr/bin/env bash
# Removes everything in the right order. Load balancers created by Kubernetes (Ingress, Grafana,
# ArgoCD, Kafka internal LBs) must be deleted BEFORE the cluster and network, otherwise their
# forwarding rules / NEGs block `terraform destroy` of the VPC.
#   ./scripts/teardown.sh <PROJECT_ID> <GIT_REPO_URL>
set -uo pipefail
PROJECT_ID="${1:?project id}"
GIT_REPO_URL="${2:?git repo url}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STATE_BUCKET="${PROJECT_ID}-tfstate"
ZONE="$(terraform -chdir="$ROOT/infra/terraform/2-infra" output -raw zone 2>/dev/null || echo asia-south1-a)"

if gcloud container clusters get-credentials sb-gke --zone "$ZONE" --project "$PROJECT_ID" 2>/dev/null; then
  echo "== stopping GitOps and deleting workloads (cascade via ArgoCD finalizers)"
  kubectl -n argocd delete application root --wait=true --timeout=120s 2>/dev/null
  kubectl -n argocd delete application smart-building kafka-cluster monitoring --wait=true --timeout=600s 2>/dev/null
  kubectl -n argocd delete application strimzi-operator --wait=true --timeout=300s 2>/dev/null
  kubectl delete ingress --all -n smart-building --wait=true --timeout=300s 2>/dev/null
  kubectl -n argocd patch svc argocd-server -p '{"spec":{"type":"ClusterIP"}}' 2>/dev/null
  echo "== waiting for load balancers to be released"
  sleep 90
fi

echo "== terraform destroy: platform"
terraform -chdir="$ROOT/infra/terraform/3-platform" init -input=false -backend-config="bucket=${STATE_BUCKET}" >/dev/null
terraform -chdir="$ROOT/infra/terraform/3-platform" destroy -auto-approve -var project_id="$PROJECT_ID" \
  -var state_bucket="$STATE_BUCKET" -var git_repo_url="$GIT_REPO_URL"

echo "== terraform destroy: infrastructure"
terraform -chdir="$ROOT/infra/terraform/2-infra" init -input=false -backend-config="bucket=${STATE_BUCKET}" >/dev/null
terraform -chdir="$ROOT/infra/terraform/2-infra" destroy -auto-approve

echo "Done. (The state bucket from 1-bootstrap is kept; delete it with: terraform -chdir=infra/terraform/1-bootstrap destroy -var project_id=$PROJECT_ID)"
