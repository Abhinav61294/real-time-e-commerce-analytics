output "project_id" {
  description = "GCP project ID"
  value       = var.project_id
}

output "bigquery_dataset" {
  description = "BigQuery analytics dataset"
  value       = google_bigquery_dataset.ecommerce_analytics.dataset_id
}

output "bigquery_events_table" {
  description = "BigQuery events table"
  value       = google_bigquery_table.events.table_id
}