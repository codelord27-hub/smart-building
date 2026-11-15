# Object storage (Cloud Storage)
resource "google_storage_bucket" "raw" {
  name                        = "${var.project_id}-sb-raw"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  lifecycle_rule {
    condition { age = 30 }
    action { type = "Delete" }
  }
  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "functions" {
  name                        = "${var.project_id}-sb-functions"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  depends_on                  = [google_project_service.apis]
}

resource "google_storage_bucket" "flink" {
  name                        = "${var.project_id}-sb-flink"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  depends_on                  = [google_project_service.apis]
}

resource "google_storage_bucket" "dataproc_staging" {
  name                        = "${var.project_id}-sb-dataproc-staging"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  depends_on                  = [google_project_service.apis]
}

resource "google_storage_bucket" "dataproc_temp" {
  name                        = "${var.project_id}-sb-dataproc-temp"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  depends_on                  = [google_project_service.apis]
}

# Container images
resource "google_artifact_registry_repository" "images" {
  repository_id = "smart-building"
  location      = var.region
  format        = "DOCKER"
  description   = "Smart Building microservice images"
  depends_on    = [google_project_service.apis]
}
