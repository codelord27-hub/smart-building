variable "project_id" {
  description = "GCP project id"
  type        = string
}

variable "region" {
  description = "Region for all regional resources"
  type        = string
  default     = "asia-south1"
}

variable "zone" {
  description = "Zone for the zonal GKE cluster and the Dataproc cluster"
  type        = string
  default     = "asia-south1-a"
}

variable "github_repo" {
  description = "GitHub repository allowed to deploy via Workload Identity Federation, as owner/name"
  type        = string
}

# ---- sizing. Defaults fit the free-trial limit of 8 vCPUs in total
# (GKE 1 x e2-standard-4 + Dataproc 1 x e2-standard-4). See sizing-paid.tfvars.example for more headroom.
variable "gke_machine_type" {
  type    = string
  default = "e2-standard-4"
}

variable "gke_min_nodes" {
  type    = number
  default = 1
}

variable "gke_max_nodes" {
  type    = number
  default = 3
}

variable "dataproc_machine_type" {
  type    = string
  default = "e2-standard-4"
}

variable "sql_tier" {
  type    = string
  default = "db-g1-small"
}

# Fixed internal IPs for the Kafka external listener (internal load balancers) so that the
# Flink job on Dataproc has a stable bootstrap address. They must be inside subnet_cidr and
# match k8s/platform/kafka/kafka-cluster.yaml.
variable "kafka_bootstrap_ip" {
  type    = string
  default = "10.10.15.240"
}

variable "kafka_broker0_ip" {
  type    = string
  default = "10.10.15.241"
}

variable "subnet_cidr" {
  type    = string
  default = "10.10.0.0/20"
}
