# Managed Kubernetes (GKE Standard, zonal) hosting the stateless microservices, Kafka (Strimzi),
# ArgoCD and the Prometheus/Grafana stack.
resource "google_service_account" "gke_nodes" {
  account_id   = "sb-gke-nodes"
  display_name = "GKE node service account"
}

resource "google_project_iam_member" "gke_nodes" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/monitoring.viewer",
    "roles/stackdriver.resourceMetadata.writer",
    "roles/artifactregistry.reader",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.gke_nodes.email}"
}

resource "google_container_cluster" "gke" {
  name                     = "sb-gke"
  location                 = var.zone
  network                  = google_compute_network.vpc.id
  subnetwork               = google_compute_subnetwork.subnet.id
  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = false
  networking_mode          = "VPC_NATIVE"

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  release_channel {
    channel = "REGULAR"
  }

  # Centralised logging: all container stdout/stderr -> Cloud Logging
  logging_config {
    enable_components = ["SYSTEM_COMPONENTS", "WORKLOADS"]
  }

  monitoring_config {
    enable_components = ["SYSTEM_COMPONENTS"]
    managed_prometheus {
      enabled = false # we run our own Prometheus (kube-prometheus-stack)
    }
  }

  addons_config {
    http_load_balancing {
      disabled = false # GKE Ingress -> public HTTP load balancer
    }
    horizontal_pod_autoscaling {
      disabled = false
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_container_node_pool" "primary" {
  name               = "primary"
  cluster            = google_container_cluster.gke.id
  location           = var.zone
  initial_node_count = var.gke_min_nodes

  autoscaling {
    min_node_count = var.gke_min_nodes
    max_node_count = var.gke_max_nodes
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  # never create extra (surge) nodes during upgrades - keeps us inside small CPU quotas
  upgrade_settings {
    max_surge       = 0
    max_unavailable = 1
  }

  node_config {
    machine_type    = var.gke_machine_type
    disk_size_gb    = 50
    disk_type       = "pd-balanced"
    service_account = google_service_account.gke_nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    labels          = { app = "smart-building" }

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }

  depends_on = [google_project_iam_member.gke_nodes]
}
