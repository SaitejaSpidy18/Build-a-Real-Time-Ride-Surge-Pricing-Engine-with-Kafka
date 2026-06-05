import os
import json
import uuid
from datetime import datetime

import psycopg2
from confluent_kafka import Consumer, Producer

KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "kafka:9092")
GROUP_ID = os.getenv("PRICING_GROUP_ID", "pricing-engine-group")
DATABASE_URL = os.getenv("DATABASE_URL")

def make_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BROKER_URL,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

def make_producer():
    return Producer({"bootstrap.servers": KAFKA_BROKER_URL})

def get_db_conn():
    return psycopg2.connect(DATABASE_URL)

def process_event(conn, producer, event):
    city_zone = event["city_zone"]
    active_drivers = event["active_drivers"]
    pending_requests = event["pending_requests"]
    surge = max(1.0, pending_requests / max(1, active_drivers))

    if surge > 2.0:
        alert = {
            "alert_id": str(uuid.uuid4()),
            "city_zone": city_zone,
            "surge_multiplier": float(surge),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        producer.produce(
            topic="surge-alerts",
            key=city_zone.encode("utf-8"),
            value=json.dumps(alert).encode("utf-8"),
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO surge_alerts (alert_id, city_zone, surge_multiplier, timestamp)
                VALUES (%s, %s, %s, %s)
                """,
                (alert["alert_id"], alert["city_zone"], alert["surge_multiplier"], datetime.utcnow()),
            )
        conn.commit()

def main():
    consumer = make_consumer()
    producer = make_producer()
    conn = get_db_conn()
    consumer.subscribe(["ride-requests"])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            event = json.loads(msg.value().decode("utf-8"))
            process_event(conn, producer, event)
            consumer.commit(msg)
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()
