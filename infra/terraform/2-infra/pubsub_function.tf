# Pub/Sub topic + serverless Alert Service (Cloud Functions 2nd gen, event-driven via Eventarc)
resource "google_pubsub_topic" "alerts" {
  name                       = "building-alerts"
  message_retention_duration = "86400s"
  depends_on                 = [google_project_service.apis]
}

resource "google_service_account" "alert_fn" {
  account_id   = "sb-alert-fn"
  display_name = "Alert Service function runtime"
}

resource "google_service_account" "alert_trigger" {
  account_id   = "sb-alert-trigger"
  display_name = "Eventarc trigger identity for the Alert Service"
}

resource "google_service_account" "fn_build" {
  account_id   = "sb-fn-build"
  display_name = "Cloud Build identity for building the function"
}

resource "google_project_iam_member" "alert_fn" {
  for_each = toset(["roles/datastore.user", "roles/logging.logWriter"])
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.alert_fn.email}"
}

resource "google_project_iam_member" "alert_trigger" {
  for_each = toset(["roles/run.invoker", "roles/eventarc.eventReceiver"])
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.alert_trigger.email}"
}

resource "google_project_iam_member" "fn_build" {
  for_each = toset(["roles/cloudbuild.builds.builder", "roles/logging.logWriter", "roles/artifactregistry.writer",
  "roles/storage.objectViewer"])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.fn_build.email}"
}

# IAM changes take a little while to propagate before Cloud Build / Eventarc can use them
resource "time_sleep" "iam_propagation" {
  create_duration = "60s"
  depends_on = [google_project_iam_member.alert_fn, google_project_iam_member.alert_trigger,
  google_project_iam_member.fn_build]
}

data "archive_file" "alert_fn" {
  type        = "zip"
  source_dir  = "${path.module}/../../../functions/alert-service"
  output_path = "${path.module}/.build/alert-service.zip"
  excludes    = ["__pycache__", "__pycache__/*"]
}

resource "google_storage_bucket_object" "alert_fn" {
  name   = "alert-service-${data.archive_file.alert_fn.output_md5}.zip"
  bucket = google_storage_bucket.functions.name
  source = data.archive_file.alert_fn.output_path
}

resource "google_cloudfunctions2_function" "alert" {
  name        = "alert-service"
  location    = var.region
  description = "Stores building alerts from Pub/Sub in Firestore and logs them"

  build_config {
    runtime         = "python312"
    entry_point     = "handle_alert"
    service_account = google_service_account.fn_build.id
    source {
      storage_source {
        bucket = google_storage_bucket.functions.name
        object = google_storage_bucket_object.alert_fn.name
      }
    }
  }

  service_config {
    min_instance_count    = 0
    max_instance_count    = 5
    available_memory      = "256M"
    timeout_seconds       = 60
    ingress_settings      = "ALLOW_ALL" # still IAM-protected: only the Eventarc trigger SA may invoke
    service_account_email = google_service_account.alert_fn.email
    environment_variables = {
      GCP_PROJECT = var.project_id
    }
  }

  event_trigger {
    trigger_region        = var.region
    event_type            = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic          = google_pubsub_topic.alerts.id
    retry_policy          = "RETRY_POLICY_RETRY"
    service_account_email = google_service_account.alert_trigger.email
  }

  depends_on = [time_sleep.iam_propagation, google_firestore_database.default]
}
