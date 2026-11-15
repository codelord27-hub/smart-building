output "project_id" { value = var.project_id }
output "region" { value = var.region }
output "zone" { value = var.zone }

output "gke_cluster_name" { value = google_container_cluster.gke.name }
output "gke_endpoint" {
  value     = google_container_cluster.gke.endpoint
  sensitive = true
}
output "gke_ca_certificate" {
  value     = google_container_cluster.gke.master_auth[0].cluster_ca_certificate
  sensitive = true
}

output "image_repository" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}

output "sql_private_ip" { value = google_sql_database_instance.pg.private_ip_address }
output "sql_user" { value = google_sql_user.app.name }
output "sql_password" {
  value     = random_password.db.result
  sensitive = true
}

output "raw_bucket" { value = google_storage_bucket.raw.name }
output "flink_bucket" { value = google_storage_bucket.flink.name }
output "alerts_topic" { value = google_pubsub_topic.alerts.name }
output "alert_function" { value = google_cloudfunctions2_function.alert.name }
output "workloads_gsa" { value = google_service_account.workloads.email }

output "dataproc_cluster" { value = google_dataproc_cluster.flink.name }
output "kafka_external_bootstrap" { value = "${var.kafka_bootstrap_ip}:9094" }

# values for the GitHub repository variables (Settings -> Secrets and variables -> Actions -> Variables)
output "github_actions_variables" {
  value = {
    GCP_PROJECT_ID     = var.project_id
    GCP_REGION         = var.region
    GCP_WIF_PROVIDER   = google_iam_workload_identity_pool_provider.github.name
    GCP_CI_SA          = google_service_account.ci.email
    IMAGE_REPOSITORY   = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
    FLINK_BUCKET       = google_storage_bucket.flink.name
    DATAPROC_CLUSTER   = google_dataproc_cluster.flink.name
    KAFKA_EXTERNAL_BOOTSTRAP = "${var.kafka_bootstrap_ip}:9094"
  }
}
