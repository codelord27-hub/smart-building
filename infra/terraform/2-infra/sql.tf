# Managed SQL (Cloud SQL for PostgreSQL, private IP only) - relational data:
# building model (registry), HVAC units + participant log (hvac), energy budget/reservations
# (energy) and the 2PC coordinator log (optimizer). One database per service.
resource "random_password" "db" {
  length  = 24
  special = false
}

resource "google_sql_database_instance" "pg" {
  name                = "sb-postgres"
  region              = var.region
  database_version    = "POSTGRES_16"
  deletion_protection = false

  settings {
    tier              = var.sql_tier
    edition           = "ENTERPRISE"
    availability_type = "ZONAL"
    disk_size         = 10
    disk_type         = "PD_SSD"

    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.vpc.id
    }

    backup_configuration {
      enabled = false # demo project - keeps cost down
    }

    database_flags {
      name  = "max_connections"
      value = "100"
    }
  }

  depends_on = [google_service_networking_connection.private_services]
}

resource "google_sql_database" "dbs" {
  for_each = toset(["registry", "energy", "hvac", "optimizer"])
  name     = each.value
  instance = google_sql_database_instance.pg.name
}

resource "google_sql_user" "app" {
  name     = "sbapp"
  instance = google_sql_database_instance.pg.name
  password = random_password.db.result
}
