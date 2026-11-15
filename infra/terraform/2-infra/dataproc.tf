# Managed cluster for the real-time stream processing service (Apache Flink on Dataproc / YARN).
# Single-node cluster, no public IP, same VPC as GKE so it reaches Kafka's internal load balancer.
resource "google_service_account" "dataproc" {
  account_id   = "sb-dataproc"
  display_name = "Dataproc (Flink) VM service account"
}

resource "google_project_iam_member" "dataproc" {
  for_each = toset(["roles/dataproc.worker", "roles/logging.logWriter", "roles/monitoring.metricWriter"])
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.dataproc.email}"
}

resource "google_storage_bucket_iam_member" "dataproc_buckets" {
  for_each = {
    staging = google_storage_bucket.dataproc_staging.name
    temp    = google_storage_bucket.dataproc_temp.name
    flink   = google_storage_bucket.flink.name
  }
  bucket = each.value
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc.email}"
}

resource "google_dataproc_cluster" "flink" {
  name   = "sb-flink"
  region = var.region

  cluster_config {
    staging_bucket = google_storage_bucket.dataproc_staging.name
    temp_bucket    = google_storage_bucket.dataproc_temp.name

    gce_cluster_config {
      zone                   = var.zone
      subnetwork             = google_compute_subnetwork.subnet.self_link
      internal_ip_only       = true
      service_account        = google_service_account.dataproc.email
      service_account_scopes = ["cloud-platform"]
      tags                   = ["dataproc"]
    }

    master_config {
      num_instances = 1
      machine_type  = var.dataproc_machine_type
      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 50
      }
    }

    worker_config {
      num_instances = 0 # single-node cluster
    }

    software_config {
      image_version       = "2.2-debian12" # Flink 1.17, Java 11
      optional_components = ["FLINK"]
      override_properties = {
        "dataproc:dataproc.allow.zero.workers"                    = "true"
        "dataproc:dataproc.logging.stackdriver.enable"            = "true"
        "dataproc:dataproc.logging.stackdriver.job.driver.enable" = "true"
        "dataproc:dataproc.logging.stackdriver.job.yarn.container.enable" = "true"
        "dataproc:jobs.file-backed-output.enable"                 = "true"
        "flink:taskmanager.numberOfTaskSlots"                     = "2"
        "flink:jobmanager.memory.process.size"                    = "1024m"
        "flink:taskmanager.memory.process.size"                   = "2048m"
      }
    }

    endpoint_config {
      enable_http_port_access = true # Component Gateway: YARN / Flink UI in the console
    }
  }

  depends_on = [google_project_iam_member.dataproc, google_storage_bucket_iam_member.dataproc_buckets]
}
