# Identity for the pods (GKE Workload Identity): Kubernetes SA smart-building/sb-app acts as this GSA.
resource "google_service_account" "workloads" {
  account_id   = "sb-workloads"
  display_name = "Smart Building microservices (GKE Workload Identity)"
}

resource "google_project_iam_member" "workloads" {
  for_each = toset(["roles/datastore.user", "roles/pubsub.publisher"])
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.workloads.email}"
}

resource "google_storage_bucket_iam_member" "workloads_raw" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.workloads.email}"
}

resource "google_service_account_iam_member" "workload_identity" {
  service_account_id = google_service_account.workloads.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[smart-building/sb-app]"
  depends_on         = [google_container_cluster.gke]
}

# ---------------------------------------------------------------------------------------------
# CI/CD: GitHub Actions authenticates with Workload Identity Federation (no JSON keys).
resource "random_id" "wif" {
  byte_length = 3
}

resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-${random_id.wif.hex}" # pools are soft-deleted for 30 days -> unique id
  display_name              = "GitHub Actions"
  depends_on                = [google_project_service.apis]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github"
  display_name                       = "GitHub OIDC"
  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }
  attribute_condition = "assertion.repository == '${var.github_repo}'"
  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account" "ci" {
  account_id   = "sb-github-ci"
  display_name = "GitHub Actions CI/CD"
}

resource "google_service_account_iam_member" "ci_wif" {
  service_account_id = google_service_account.ci.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repo}"
}

resource "google_artifact_registry_repository_iam_member" "ci_push" {
  location   = google_artifact_registry_repository.images.location
  repository = google_artifact_registry_repository.images.name
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.ci.email}"
}

resource "google_storage_bucket_iam_member" "ci_flink" {
  bucket = google_storage_bucket.flink.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.ci.email}"
}

resource "google_project_iam_member" "ci_dataproc" {
  project = var.project_id
  role    = "roles/dataproc.editor" # submit / cancel Flink jobs
  member  = "serviceAccount:${google_service_account.ci.email}"
}
