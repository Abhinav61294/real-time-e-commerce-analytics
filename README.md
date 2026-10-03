# Real-Time E-Commerce Analytics Platform

A real-time e-commerce analytics platform built on Google Cloud Platform (GCP).

The project simulates e-commerce events, streams them through Pub/Sub, processes and validates them using Apache Beam on Dataflow, stores analytical data in BigQuery, transforms it using Dataform, and visualizes business metrics through Looker Studio.

## Architecture

Event Generator
      |
      v
Google Cloud Pub/Sub
      |
      v
Apache Beam / Dataflow
      |
      +-------------------+
      |                   |
      v                   v
Valid Events        Invalid Events
      |
      v
BigQuery
      |
      v
Dataform
      |
      v
Analytics Tables
      |
      v
Looker Studio

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
2. Events are published to a Google Cloud Pub/Sub topic.
3. A Pub/Sub subscription provides events to the Dataflow pipeline.
4. Apache Beam consumes and parses the streaming events.
5. Events are validated against required business rules.
6. Invalid events are separated from the valid event stream.
7. Valid events are transformed and enriched.
8. Revenue is calculated using price and quantity.
9. Event timestamps are used for event-time processing.
10. Processed events are written to BigQuery.
11. Dataform creates analytical tables and data-quality assertions.
12. Looker Studio visualizes real-time business metrics.

## Event Validation

The streaming pipeline validates:

- Required fields
- Event ID
- Event type
- Device type
- Price
- Quantity

Invalid events are separated from the valid event stream instead of being loaded into the main analytical dataset.

Dataform also performs data-quality assertions against the BigQuery events table.

Assertions check for:

- Missing event IDs
- Missing event types
- Negative revenue

## Event Processing

The pipeline supports common e-commerce event types:

- `product_view`
- `add_to_cart`
- `purchase`
- `payment`
- `refund`

Event timestamps are used for event-time processing, allowing events to be analyzed based on when they actually occurred rather than only when they were received.

## BigQuery Analytics

Processed events are stored in BigQuery for analytical querying.

Analytics are available by:

- Event type
- Device type
- Country
- Event date
- Revenue
- Event count

Revenue is calculated from:

    quantity × price

The BigQuery layer provides the foundation for downstream analytical transformations and dashboards.

## Dataform

Dataform manages analytical transformations using SQLX.

Models created:

- `daily_event_summary_dataform`
- `device_summary_dataform`
- `country_summary_dataform`

Dataform also includes a data-quality assertion for the BigQuery events data.

## Looker Studio

Looker Studio is used to visualize the processed e-commerce data.

The dashboard provides analysis of:

- Event volume
- Revenue
- Event types
- Device activity
- Geographic performance
- Daily trends

## Streaming Pipeline

The streaming architecture is designed around:

- Pub/Sub for event ingestion
- Apache Beam for stream processing
- Dataflow for managed execution
- BigQuery for analytical storage

The pipeline separates invalid events from valid events while processing the stream.

## Infrastructure as Code

Terraform configuration is included under:

    terraform/

The project also includes GitHub Actions workflow configuration for CI/CD.

Infrastructure and project configuration are maintained in GitHub to support reproducible development and deployment workflows.

## Project Structure

real-time-e-commerce-analytics/
│
├── dataflow/
├── data/
├── dataform/
├── terraform/
├── tests/
├── docs/
├── README.md
└── .gitignore

## Key Engineering Concepts

- Real-time data ingestion
- Event-driven architecture
- Pub/Sub streaming
- Apache Beam
- Google Cloud Dataflow
- Event-time processing
- Streaming data validation
- Invalid event separation
- BigQuery analytics
- Dataform transformations
- Data-quality assertions
- Looker Studio visualization
- Infrastructure as Code
- CI/CD

## End-to-End Flow

Python Event Generator
        |
        v
Google Cloud Pub/Sub
        |
        v
Apache Beam / Dataflow
        |
        +---- Invalid Events
        |
        +---- Valid Events
                  |
                  v
              BigQuery
                  |
                  v
               Dataform
                  |
                  v
            Analytics Tables
                  |
                  v
            Looker Studio
