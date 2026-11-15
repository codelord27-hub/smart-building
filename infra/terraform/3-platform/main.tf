# Stage 3 - Kubernetes bootstrap (still Infrastructure as Code):
#   namespaces, Workload Identity service account, platform ConfigMap, DB credentials Secret,
#   Grafana admin Secret, ArgoCD (Helm) and the ArgoCD "root" Application (app-of-apps).
# After this, every Kubernetes workload is deployed by ArgoCD from Git - never with kubectl apply.
terraform {
  required_version = ">= 1.5"
  backend "gcs" {
    prefix = "smart-building/platform"
  }
  required_providers {
    google     = { source = "hashicorp/google", version = "~> 6.0" }
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.31" }
    helm       = { source = "hashicorp/helm", version = "~> 2.14" }
    random     = { source = "hashicorp/random", version = "~> 3.6" }
  }
}

variable "project_id" { type = string }
variable "state_bucket" { type = string }
variable "git_repo_url" {
  description = "HTTPS URL of the GitHub repository ArgoCD tracks, e.g. https://github.com/me/smart-building.git"
  type        = string
}
variable "git_revision" {
  type    = string
  default = "main"
}
variable "git_token" {
  description = "Only for a PRIVATE repository: GitHub token with read access (leave empty for public repos)"
  type        = string
  default     = ""
  sensitive   = true
}
variable "argocd_chart_version" {
  type    = string
  default = "9.5.15"
}

data "terraform_remote_state" "infra" {
  backend = "gcs"
  config = {
    bucket = var.state_bucket
    prefix = "smart-building/infra"
  }
}

locals {
  infra = data.terraform_remote_state.infra.outputs
}

provider "google" {
  project = var.project_id
}

data "google_client_config" "current" {}

provider "kubernetes" {
  host                   = "https://${local.infra.gke_endpoint}"
  token                  = data.google_client_config.current.access_token
  cluster_ca_certificate = base64decode(local.infra.gke_ca_certificate)
}

provider "helm" {
  kubernetes {
    host                   = "https://${local.infra.gke_endpoint}"
    token                  = data.google_client_config.current.access_token
    cluster_ca_certificate = base64decode(local.infra.gke_ca_certificate)
  }
}

# ------------------------------------------------------------------ namespaces
resource "kubernetes_namespace" "ns" {
  for_each = toset(["smart-building", "kafka", "monitoring", "argocd"])
  metadata {
    name = each.value
  }
}

# ------------------------------------------------------------------ app identity + configuration
resource "kubernetes_service_account" "app" {
  metadata {
    name      = "sb-app"
    namespace = kubernetes_namespace.ns["smart-building"].metadata[0].name
    annotations = {
      "iam.gke.io/gcp-service-account" = local.infra.workloads_gsa
    }
  }
}

resource "kubernetes_config_map" "platform" {
  metadata {
    name      = "platform-config"
    namespace = kubernetes_namespace.ns["smart-building"].metadata[0].name
  }
  data = {
    GCP_PROJECT             = var.project_id
    RAW_BUCKET              = local.infra.raw_bucket
    PUBSUB_ALERTS_TOPIC     = local.infra.alerts_topic
    KAFKA_BOOTSTRAP         = "sb-kafka-kafka-bootstrap.kafka.svc:9092"
    TOPIC_SENSOR_READINGS   = "sensor-readings"
    TOPIC_ENERGY_EVENTS     = "energy-events"
    TOPIC_ANALYTICS_RESULTS = "energy-analytics-results"
    DB_HOST                 = local.infra.sql_private_ip
    DB_PORT                 = "5432"
    DB_POOL_MAX             = "5"
    LOG_LEVEL               = "INFO"
  }
}

resource "kubernetes_secret" "db" {
  metadata {
    name      = "db-credentials"
    namespace = kubernetes_namespace.ns["smart-building"].metadata[0].name
  }
  data = {
    DB_USER     = local.infra.sql_user
    DB_PASSWORD = local.infra.sql_password
  }
}

resource "random_password" "grafana" {
  length  = 16
  special = false
}

resource "kubernetes_secret" "grafana_admin" {
  metadata {
    name      = "grafana-admin"
    namespace = kubernetes_namespace.ns["monitoring"].metadata[0].name
  }
  data = {
    "admin-user"     = "admin"
    "admin-password" = random_password.grafana.result
  }
}

# ------------------------------------------------------------------ GitOps controller
resource "helm_release" "argocd" {
  name       = "argocd"
  namespace  = kubernetes_namespace.ns["argocd"].metadata[0].name
  repository = "https://argoproj.github.io/argo-helm"
  chart      = "argo-cd"
  version    = var.argocd_chart_version
  timeout    = 900
  wait       = true

  values = [yamlencode({
    configs = {
      params = { "server.insecure" = true }
      cm     = { "timeout.reconciliation" = "60s" }
    }
    server = {
      service = { type = "LoadBalancer" }
    }
    dex = { enabled = false }
  })]
}

resource "kubernetes_secret" "repo" {
  count = var.git_token == "" ? 0 : 1
  metadata {
    name      = "smart-building-repo"
    namespace = kubernetes_namespace.ns["argocd"].metadata[0].name
    labels    = { "argocd.argoproj.io/secret-type" = "repository" }
  }
  data = {
    type     = "git"
    url      = var.git_repo_url
    username = "git"
    password = var.git_token
  }
}

# root Application (app-of-apps) -> k8s/argocd/apps in Git
resource "helm_release" "root_app" {
  name      = "argocd-root-app"
  namespace = kubernetes_namespace.ns["argocd"].metadata[0].name
  chart     = "${path.module}/charts/root-app"
  values = [yamlencode({
    repoURL        = var.git_repo_url
    targetRevision = var.git_revision
    path           = "k8s/argocd/apps"
  })]
  depends_on = [helm_release.argocd, kubernetes_secret.repo]
}

output "grafana_admin_password" {
  value     = random_password.grafana.result
  sensitive = true
}
