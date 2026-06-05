import os
import psycopg2
from fastapi import FastAPI
from fastapi.responses import JSONResponse

DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI()

def get_conn():
    return psycopg2.connect(DATABASE_URL)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/api/zones/live")
def zones_live():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT city_zone, active_drivers, pending_requests, surge_multiplier, last_updated
                FROM zone_stats
                ORDER BY city_zone
            """)
            rows = cur.fetchall()

        zones = [
            {
                "city_zone": r[0],
                "active_drivers": r[1],
                "pending_requests": r[2],
                "surge_multiplier": float(r[3]),
                "last_updated": r[4].isoformat()
            }
            for r in rows
        ]
        return JSONResponse(content={"zones": zones})
    finally:
        conn.close()

@app.get("/api/alerts/recent")
def alerts_recent(limit: int = 20):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT alert_id, city_zone, surge_multiplier, timestamp
                FROM surge_alerts
                ORDER BY timestamp DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()

        alerts = [
            {
                "alert_id": str(r[0]),
                "city_zone": r[1],
                "surge_multiplier": float(r[2]),
                "timestamp": r[3].isoformat()
            }
            for r in rows
        ]
        return JSONResponse(content={"alerts": alerts})
    finally:
        conn.close()