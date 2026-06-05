import os
import json
import time
from datetime import datetime

import psycopg2
from confluent_kafka import Consumer

KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "kafka:9092")
GROUP_ID = os.getenv("ANALYTICS_GROUP_ID", "analytics-recorder-group")
DATABASE_URL = os.getenv("DATABASE_URL")

CITY_ZONES = [
    "downtown",
    "airport",
    "suburbs-north",
    "suburbs-south",
    "business-district",
    "stadium-complex",
]

def make_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BROKER_URL,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

def get_db_conn():
    return psycopg2.connect(DATABASE_URL)

def upsert_zone_stats(conn, state):
    with conn.cursor() as cur:
        for zone, z in state.items():
            cur.execute(
                """
                INSERT INTO zone_stats (city_zone, active_drivers, pending_requests, surge_multiplier, last_updated)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (city_zone)
                DO UPDATE SET
                  active_drivers = EXCLUDED.active_drivers,
                  pending_requests = EXCLUDED.pending_requests,
                  surge_multiplier = EXCLUDED.surge_multiplier,
                  last_updated = EXCLUDED.last_updated
                """,
                (zone, z["active_drivers"], z["pending_requests"], z["surge_multiplier"], datetime.utcnow()),
            )
    conn.commit()

def main():
    consumer = make_consumer()
    conn = get_db_conn()

    zone_state = {zone: {"active_drivers": 0, "pending_requests": 0, "surge_multiplier": 1.0} for zone in CITY_ZONES}
    last_flush = time.time()
    consumer.subscribe(["ride-requests"])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                if time.time() - last_flush >= 30:
                    upsert_zone_stats(conn, zone_state)
                    last_flush = time.time()
                continue

            event = json.loads(msg.value().decode("utf-8"))
            zone = event["city_zone"]
            ad = event["active_drivers"]
            pr = event["pending_requests"]
            surge = max(1.0, pr / max(1, ad))
            zone_state[zone] = {
                "active_drivers": ad,
                "pending_requests": pr,
                "surge_multiplier": surge,
            }

            if time.time() - last_flush >= 30:
                upsert_zone_stats(conn, zone_state)
                last_flush = time.time()
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()
