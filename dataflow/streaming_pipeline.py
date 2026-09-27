import json
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery


PROJECT_ID = "real-time-e-commerce-analytics"
REGION = "asia-southeast1"

STAGING_BUCKET = "gs://real-time-e-commerce-analytics-staging"

SUBSCRIPTION = (
    f"projects/{PROJECT_ID}/subscriptions/"
    "ecommerce-dataflow-sub"
)

BIGQUERY_TABLE = (
    f"{PROJECT_ID}:ecommerce_analytics.events"
)

VALID_EVENT_TYPES = {
    "product_view",
    "add_to_cart",
    "purchase",
    "payment",
    "refund",
}

VALID_DEVICES = {
    "mobile",
    "web",
    "tablet",
}

INVALID_TAG = "invalid"


def parse_event(message):
    return json.loads(message.decode("utf-8"))


class ValidateEvent(beam.DoFn):

    def process(self, event):

        required_fields = [
            "event_id",
            "user_id",
            "product_id",
            "event_type",
            "price",
            "quantity",
            "timestamp",
            "device",
            "country",
        ]

        for field in required_fields:
            if field not in event or event[field] in (None, ""):
                yield beam.pvalue.TaggedOutput(
                    INVALID_TAG,
                    event
                )
                return

        if event["event_type"] not in VALID_EVENT_TYPES:
            yield beam.pvalue.TaggedOutput(
                INVALID_TAG,
                event
            )
            return

        if event["device"] not in VALID_DEVICES:
            yield beam.pvalue.TaggedOutput(
                INVALID_TAG,
                event
            )
            return

        if event["price"] <= 0:
            yield beam.pvalue.TaggedOutput(
                INVALID_TAG,
                event
            )
            return

        if event["quantity"] <= 0:
            yield beam.pvalue.TaggedOutput(
                INVALID_TAG,
                event
            )
            return

        yield event


def transform_event(event):

    event["revenue"] = round(
        event["price"] * event["quantity"],
        2
    )

    event["event_date"] = event["timestamp"][:10]

    return event


def assign_event_timestamp(event):

    timestamp = datetime.fromisoformat(
        event["timestamp"].replace("Z", "+00:00")
    )

    return beam.window.TimestampedValue(
        event,
        timestamp.timestamp()
    )


def add_revenue_key(event):
    return "total_revenue", event["revenue"]


def format_revenue_result(result):

    key, total_revenue = result

    return {
        "metric": key,
        "total_revenue": round(total_revenue, 2)
    }


def run():

    pipeline_options = [
        "--runner=DataflowRunner",
        f"--project={PROJECT_ID}",
        f"--region={REGION}",
        f"--staging_location={STAGING_BUCKET}/staging",
        f"--temp_location={STAGING_BUCKET}/temp",
        "--job_name=realtime-ecommerce-streaming",
        "--save_main_session",
        "--streaming",
        "--machine_type=e2-standard-2",
        "--num_workers=1",
        "--max_num_workers=1",
    ]

    options = PipelineOptions(pipeline_options)

    standard_options = options.view_as(StandardOptions)
    standard_options.streaming = True

    with beam.Pipeline(options=options) as pipeline:

        # ---------------------------------------------
        # 1. Read events from Pub/Sub
        # ---------------------------------------------

        events = (
            pipeline
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(
                subscription=SUBSCRIPTION
            )
            | "Parse JSON" >> beam.Map(parse_event)
        )

        # ---------------------------------------------
        # 2. Validate events
        # ---------------------------------------------

        validated_events = (
            events
            | "Validate Events" >> beam.ParDo(
                ValidateEvent()
            ).with_outputs(
                INVALID_TAG,
                main="valid"
            )
        )

        valid_events = validated_events.valid

        invalid_events = validated_events[INVALID_TAG]

        # ---------------------------------------------
        # 3. Transform valid events
        # ---------------------------------------------

        transformed_events = (
            valid_events
            | "Transform Events" >> beam.Map(
                transform_event
            )
        )

        # ---------------------------------------------
        # 4. Write valid events to BigQuery
        # ---------------------------------------------

        (
            transformed_events
            | "Write Events To BigQuery" >> WriteToBigQuery(
                BIGQUERY_TABLE,
                schema={
                    "fields": [
                        {
                            "name": "event_id",
                            "type": "STRING",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "user_id",
                            "type": "STRING",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "product_id",
                            "type": "STRING",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "event_type",
                            "type": "STRING",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "price",
                            "type": "FLOAT",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "quantity",
                            "type": "INTEGER",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "timestamp",
                            "type": "TIMESTAMP",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "device",
                            "type": "STRING",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "country",
                            "type": "STRING",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "revenue",
                            "type": "FLOAT",
                            "mode": "NULLABLE",
                        },
                        {
                            "name": "event_date",
                            "type": "DATE",
                            "mode": "NULLABLE",
                        },
                    ]
                },
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )

        # ---------------------------------------------
        # 5. Assign event time
        # ---------------------------------------------

        timestamped_events = (
            transformed_events
            | "Assign Event Time" >> beam.Map(
                assign_event_timestamp
            )
        )

        # ---------------------------------------------
        # 6. Apply 1-minute event-time windows
        # ---------------------------------------------

        windowed_events = (
            timestamped_events
            | "Apply 1 Minute Windows" >> beam.WindowInto(
                beam.window.FixedWindows(60)
            )
        )

        # ---------------------------------------------
        # 7. Create revenue key
        # ---------------------------------------------

        revenue_keyed_events = (
            windowed_events
            | "Create Revenue Key" >> beam.Map(
                add_revenue_key
            )
        )

        # ---------------------------------------------
        # 8. Calculate revenue per window
        # ---------------------------------------------

        revenue_per_window = (
            revenue_keyed_events
            | "Calculate Revenue" >> beam.CombinePerKey(
                sum
            )
        )

        # ---------------------------------------------
        # 9. Format results
        # ---------------------------------------------

        formatted_results = (
            revenue_per_window
            | "Format Revenue Result" >> beam.Map(
                format_revenue_result
            )
        )

        # ---------------------------------------------
        # 10. Print window results
        # ---------------------------------------------

        (
            formatted_results
            | "Print Window Revenue" >> beam.Map(
                lambda result: print(
                    "WINDOW REVENUE:",
                    result
                )
            )
        )

        # ---------------------------------------------
        # 11. Print invalid events
        # ---------------------------------------------

        (
            invalid_events
            | "Print Invalid Events" >> beam.Map(
                lambda event: print(
                    "INVALID:",
                    event
                )
            )
        )


if __name__ == "__main__":
    run()