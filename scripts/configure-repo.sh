#!/usr/bin/env bash
# Run ONCE after `terraform apply` of infra/terraform/2-infra, from the repository root:
#   ./scripts/configure-repo.sh https://github.com/<user>/<repo>.git
# It writes the project-specific values into the GitOps manifests and the CI config, so that
# ArgoCD and GitHub Actions point at your project. Commit and push the result.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO_URL="${1:?usage: configure-repo.sh <https git url of this repo>}"
TF_DIR="$ROOT/infra/terraform/2-infra"

out() { terraform -chdir="$TF_DIR" output -raw "$1"; }
PROJECT_ID="$(out project_id)"
REGION="$(out region)"
IMAGE_REPO="$(out image_repository)"
VARS_JSON="$(terraform -chdir="$TF_DIR" output -json github_actions_variables)"

# 1. image repository in the kustomization (tags stay as they are until CI builds images)
sed -i "s#REGION-docker.pkg.dev/PROJECT_ID/smart-building#${IMAGE_REPO}#g" "$ROOT/k8s/apps/kustomization.yaml"
# 2. Git URL tracked by ArgoCD
sed -i "s#repoURL: GIT_REPO_URL#repoURL: ${REPO_URL}#g" "$ROOT"/k8s/argocd/apps/*.yaml
# 3. CI configuration (non-secret identifiers only; authentication uses Workload Identity Federation)
python3 - "$VARS_JSON" > "$ROOT/.github/ci-config.env" <<'EOF'
import json, sys
for k, v in json.loads(sys.argv[1]).items():
    print(f"{k}={v}")
EOF

echo "Configured for project ${PROJECT_ID} (${REGION})"
echo "  images : ${IMAGE_REPO}/<service>"
echo "  git    : ${REPO_URL}"
echo "Now commit and push:  git add -A && git commit -m 'Configure for ${PROJECT_ID}' && git push"
