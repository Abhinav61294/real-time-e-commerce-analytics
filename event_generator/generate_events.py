import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from google.cloud import pubsub_v1

fake = Faker()

PROJECT_ID = "real-time-e-commerce-analytics"
TOPIC_ID = "ecommerce-events"

publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(
    PROJECT_ID,
    TOPIC_ID
)

EVENT_TYPES = [
    "product_view",
    "add_to_cart",
    "purchase",
    "payment",
    "refund",
]

DEVICES = [
    "mobile",
    "web",
    "tablet",
]

COUNTRIES = [
    "India",
    "USA",
    "UK",
    "Germany",
    "Canada",
    "Australia",
]

PRODUCTS = [
    "PROD001",
    "PROD002",
    "PROD003",
    "PROD004",
    "PROD005",
    "PROD006",
    "PROD007",
    "PROD008",
    "PROD009",
    "PROD010",
]


def generate_event():
    event_type = random.choices(
        EVENT_TYPES,
        weights=[45, 20, 20, 10, 5],
        k=1
    )[0]

    event = {
        "event_id": f"EVT-{uuid.uuid4().hex[:10].upper()}",
        "user_id": f"USER-{random.randint(1000, 9999)}",
        "product_id": random.choice(PRODUCTS),
        "event_type": event_type,
        "price": round(random.uniform(100, 5000), 2),
        "quantity": random.randint(1, 5),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device": random.choice(DEVICES),
        "country": random.choice(COUNTRIES),
    }

    return event

def introduce_bad_data(event):
    if random.random() > 0.05:
        return event

    bad_type = random.choice([
        "missing_event_id",
        "negative_price",
        "invalid_event_type",
        "zero_quantity",
        "invalid_device"
    ])

    if bad_type == "missing_event_id":
        event["event_id"] = ""

    elif bad_type == "negative_price":
        event["price"] = -100

    elif bad_type == "invalid_event_type":
        event["event_type"] = "unknown_event"

    elif bad_type == "zero_quantity":
        event["quantity"] = 0

    elif bad_type == "invalid_device":
        event["device"] = "smartwatch"

    return event

def publish_event(event):
    message = json.dumps(event).encode("utf-8")

    future = publisher.publish(
        topic_path,
        message
    )

    message_id = future.result()

    return message_id


def main():
    print("Starting e-commerce event generator...")
    print("Publishing events to Pub/Sub...")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            event = generate_event()

            event = introduce_bad_data(event)

            message_id = publish_event(event)

            print(
                f"Published | "
                f"event_id={event['event_id']} | "
                f"type={event['event_type']} | "
                f"message_id={message_id}"
            )

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nEvent generator stopped.")


if __name__ == "__main__":
    main()