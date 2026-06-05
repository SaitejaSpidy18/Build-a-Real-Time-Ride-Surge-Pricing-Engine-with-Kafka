import os
import json
import time
import uuid
from datetime import datetime
from math import sin, pi

from confluent_kafka import Producer

KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "kafka:9092")
INTERVAL = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))

CITY_ZONES = [
    "downtown",
    "airport",
    "suburbs-north",
    "suburbs-south",
    "business-district",
    "stadium-complex",
]

def make_producer():
    return Producer({"bootstrap.servers": KAFKA_BROKER_URL})

def simulate_counts(zone, t_seconds):
    base_drivers = 20
    base_requests = 15
    factor = 1 + 0.5 * sin(2 * pi * (t_seconds / 60.0))
    active_drivers = max(0, int(base_drivers * factor))
    pending_requests = max(0, int(base_requests * (2 - factor)))
    return active_drivers, pending_requests

def main():
    producer = make_producer()
    start = time.time()
    while True:
        t = time.time() - start
        for zone in CITY_ZONES:
            active_drivers, pending_requests = simulate_counts(zone, t)
            event = {
                "event_id": str(uuid.uuid4()),
                "city_zone": zone,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "active_drivers": active_drivers,
                "pending_requests": pending_requests,
            }
            producer.produce(
                topic="ride-requests",
                key=zone.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
            )
        producer.flush()
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
