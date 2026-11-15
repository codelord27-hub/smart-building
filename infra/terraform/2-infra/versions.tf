terraform {
  required_version = ">= 1.5"
  # bucket is passed at init time:  terraform init -backend-config="bucket=<PROJECT_ID>-tfstate"
  backend "gcs" {
    prefix = "smart-building/infra"
  }
  required_providers {
    google  = { source = "hashicorp/google", version = "~> 6.0" }
    random  = { source = "hashicorp/random", version = "~> 3.6" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
    time    = { source = "hashicorp/time", version = "~> 0.11" }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

data "google_project" "this" {}
