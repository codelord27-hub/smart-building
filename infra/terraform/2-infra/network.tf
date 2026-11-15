# VPC shared by GKE, Dataproc and (via private service access) Cloud SQL.
resource "google_compute_network" "vpc" {
  name                    = "sb-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.apis]
}

resource "google_compute_subnetwork" "subnet" {
  name                     = "sb-subnet"
  region                   = var.region
  network                  = google_compute_network.vpc.id
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true # Dataproc VMs have no public IP but must reach GCS / APIs

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.20.0.0/16"
  }
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.30.0.0/20"
  }
}

# Traffic inside the VPC (pods, nodes, Dataproc, internal load balancers)
resource "google_compute_firewall" "internal" {
  name          = "sb-allow-internal"
  network       = google_compute_network.vpc.name
  source_ranges = [var.subnet_cidr, "10.20.0.0/16", "10.30.0.0/20"]
  allow { protocol = "tcp" }
  allow { protocol = "udp" }
  allow { protocol = "icmp" }
}

# SSH through Identity-Aware Proxy only (no public SSH)
resource "google_compute_firewall" "iap_ssh" {
  name          = "sb-allow-iap-ssh"
  network       = google_compute_network.vpc.name
  source_ranges = ["35.235.240.0/20"]
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

# Private service access for Cloud SQL private IP
resource "google_compute_global_address" "private_services" {
  name          = "sb-private-services"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.vpc.id
}

resource "google_service_networking_connection" "private_services" {
  network                 = google_compute_network.vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_services.name]
}

# Static internal IPs used by the Strimzi external listener (GKE internal load balancers)
resource "google_compute_address" "kafka_bootstrap" {
  name         = "sb-kafka-bootstrap"
  region       = var.region
  subnetwork   = google_compute_subnetwork.subnet.id
  address_type = "INTERNAL"
  address      = var.kafka_bootstrap_ip
}

resource "google_compute_address" "kafka_broker0" {
  name         = "sb-kafka-broker-0"
  region       = var.region
  subnetwork   = google_compute_subnetwork.subnet.id
  address_type = "INTERNAL"
  address      = var.kafka_broker0_ip
}
