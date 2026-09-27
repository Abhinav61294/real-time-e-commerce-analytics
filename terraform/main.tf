terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }

  required_version = ">= 1.6.0"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------------------------------------------------------
# BigQuery Analytics Dataset
# ---------------------------------------------------------

resource "google_bigquery_dataset" "ecommerce_analytics" {
  dataset_id = "ecommerce_analytics"
  location   = "asia-southeast1"

  description = "Analytics dataset for real-time e-commerce events"
}

# ---------------------------------------------------------
# BigQuery Events Table
# ---------------------------------------------------------

resource "google_bigquery_table" "events" {
  dataset_id = google_bigquery_dataset.ecommerce_analytics.dataset_id
  table_id   = "events"

  schema = jsonencode([
    {
      name = "event_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "user_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "product_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "event_type"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "price"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "quantity"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "timestamp"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "device"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "country"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "revenue"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "event_date"
      type = "DATE"
      mode = "NULLABLE"
    }
  ])

  depends_on = [
    google_bigquery_dataset.ecommerce_analytics
  ]
}