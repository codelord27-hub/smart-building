# Stage 1 - creates the Cloud Storage bucket that stores the Terraform state of stages 2 and 3.
# Uses local state (this stage only owns one bucket).
#   terraform init && terraform apply -var project_id=<PROJECT_ID>
terraform {
  required_version = ">= 1.5"
  required_providers {
    google = { source = "hashicorp/google", version = "~> 6.0" }
  }
}

variable "project_id" { type = string }
variable "region" {
  type    = string
  default = "asia-south1"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_project_service" "bootstrap" {
  for_each           = toset(["storage.googleapis.com", "serviceusage.googleapis.com", "cloudresourcemanager.googleapis.com"])
  service            = each.value
  disable_on_destroy = false
}

resource "google_storage_bucket" "tfstate" {
  name                        = "${var.project_id}-tfstate"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  versioning { enabled = true }
  depends_on = [google_project_service.bootstrap]
}

output "state_bucket" { value = google_storage_bucket.tfstate.name }
