# Real-Time E-Commerce Analytics Platform

A real-time e-commerce analytics platform built on Google Cloud Platform (GCP).

The project ingests simulated e-commerce events through Pub/Sub, processes and validates them using Apache Beam on Dataflow, stores analytical data in BigQuery, transforms it with Dataform, and visualizes business metrics using Looker Studio.

## Architecture

Event Generator
      |
      v
Google Cloud Pub/Sub
      |
      v
Apache Beam / Dataflow
      |
      v
BigQuery
      |
      +------------------+
      |                  |
      v                  v
   Dataform        Looker Studio
      |
      v
Analytics Tables

## Tech Stack

- Python
- Google Cloud Pub/Sub
- Apache Beam
- Google Cloud Dataflow
- Google BigQuery
- Dataform
- Looker Studio
- Terraform
- GitHub Actions
- PowerShell

## Data Flow

1. Python generates simulated e-commerce events.
2. Events are published to a Pub/Sub topic.
3. Dataflow consumes events from the Pub/Sub subscription.
4. Events are parsed and validated.
5. Invalid events are separated from valid events.
6. Valid events are transformed and revenue is calculated.
7. Event timestamps are used for event-time processing.
8. Events are written to BigQuery.
9. BigQuery views provide analytical summaries.
10. Dataform creates managed analytical tables and data-quality assertions.
11. Looker Studio visualizes the resulting metrics.

## Data Validation

The streaming pipeline validates:

- Required fields
- Event type
- Device type
- Price
- Quantity

Invalid events are separated from the valid event stream.

Dataform also performs an assertion against the BigQuery events table to identify:

- Missing event IDs
- Missing event types
- Negative revenue

## BigQuery Analytics

The platform provides analytics by:

- Event type
- Device
- Country
- Event date
- Revenue
- Event count

Example event types:

- product_view
- add_to_cart
- purchase
- payment
- refund

## Dataform

Dataform manages analytical transformations using SQLX.

Models created:

- `daily_event_summary_dataform`
- `device_summary_dataform`
- `country_summary_dataform`

A data-quality assertion is also included.

## Infrastructure as Code

Terraform configuration is included under:

```text
terraform/
