# Managed NoSQL (Firestore, native mode) - high-throughput semi-structured data:
# zone_state (live occupancy/temperature), energy_analytics (Flink window results), alerts.
resource "google_firestore_database" "default" {
  name                    = "(default)"
  location_id             = var.region
  type                    = "FIRESTORE_NATIVE"
  delete_protection_state = "DELETE_PROTECTION_DISABLED"
  deletion_policy         = "DELETE"
  depends_on              = [google_project_service.apis]
}

# composite index for "analytics of one zone, newest first" (energy-monitor /api/energy/analytics?zone_id=)
resource "google_firestore_index" "analytics_by_zone" {
  database   = google_firestore_database.default.name
  collection = "energy_analytics"
  fields {
    field_path = "zone_id"
    order      = "ASCENDING"
  }
  fields {
    field_path = "window_start_ms"
    order      = "DESCENDING"
  }
}
