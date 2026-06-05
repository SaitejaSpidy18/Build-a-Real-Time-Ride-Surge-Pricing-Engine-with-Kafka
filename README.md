# Real-Time Ride Surge Pricing Engine with Kafka

A Dockerized real-time ride surge pricing simulation built with **Apache Kafka**, **PostgreSQL**, **Python**, and **FastAPI**. Kafka topics can be partitioned so events are spread across multiple buckets, and consumer groups let multiple consumers coordinate message processing across those partitions.[1][2]

## Project Overview

This project simulates a ride-hailing backend where a producer continuously generates ride demand events, a pricing engine consumer computes surge multipliers and publishes surge alerts, an analytics recorder consumer stores per-zone state in PostgreSQL, and a FastAPI service exposes live data through REST endpoints. Kafka’s partitioned topic model and consumer-group pattern make it a good fit for real-time event pipelines like surge pricing and analytics workflows.[1][2][3]

## Architecture

The system contains these services:

- **Kafka (KRaft mode):** event broker for ride requests and surge alerts.[4][5]
- **PostgreSQL:** persistent storage for zone statistics and recent surge alerts.[6]
- **Producer:** publishes ride request events keyed by `city_zone` so related events can be routed consistently by key.[1]
- **Pricing Engine Consumer:** consumes ride requests, calculates surge logic, and publishes alerts when thresholds are crossed.[2][3]
- **Analytics Recorder Consumer:** consumes events and writes the latest zone-level state to PostgreSQL for API access.[6]
- **FastAPI API:** exposes health, live zone stats, and recent alerts endpoints.[7][8]

## Topics

| Topic | Purpose | Notes |
|---|---|---|
| `ride-requests` | Incoming ride demand events | Should be created with 6 partitions to support zone-keyed distribution.[1] |
| `surge-alerts` | High-surge events published by pricing engine | Used for recent alert history and monitoring.[9] |

## Tech Stack

- Python
- Apache Kafka (Docker image: `apache/kafka`)
- PostgreSQL 16
- FastAPI
- Docker Compose

## Folder Structure

```text
.
├── analytics-recorder-consumer/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── api/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── db/
│   └── init.sql
├── pricing-engine-consumer/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── producer/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
├── .env.example
└── README.md
```

## Prerequisites

Install the following before running the project:

- Docker Desktop
- Git
- PowerShell or another terminal

Docker Compose is commonly used to start local Kafka-based multi-container environments and is a practical way to run Kafka, databases, and APIs together during development.[4][10]

## Environment Variables

Create a local `.env` file from `.env.example`:

```powershell
Copy-Item .env.example .env
```

Recommended `.env` values:

```env
KAFKA_BROKER_URL=kafka:29092
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=surge_pricing
DATABASE_URL=postgresql://postgres:postgres@database:5432/surge_pricing
PRODUCER_INTERVAL_SECONDS=2
PRICING_GROUP_ID=pricing-engine-group
ANALYTICS_GROUP_ID=analytics-recorder-group
```

Keep `.env` out of Git and commit only `.env.example`, because local secret or machine-specific files should not be pushed to remote repositories.[11][12]

## Running the Project

Start everything with Docker Compose:

```powershell
docker compose up --build -d
```

Check container status:

```powershell
docker compose ps
```

Health checks in Docker Compose help ensure containers are actually ready instead of merely started, which is especially useful when services depend on Kafka or PostgreSQL coming up first.[13][14]

## API Endpoints

### Health

```http
GET /health
```

Example:

```powershell
curl http://127.0.0.1:8000/health
```

### Live Zone Stats

```http
GET /api/zones/live
```

Example:

```powershell
curl http://127.0.0.1:8000/api/zones/live
```

### Recent Surge Alerts

```http
GET /api/alerts/recent
```

Example:

```powershell
curl http://127.0.0.1:8000/api/alerts/recent
```

FastAPI applications running in Docker should be served by Uvicorn on `0.0.0.0` so the mapped host port is reachable from outside the container.[15][16]

## Kafka Verification

Verify topics from the Kafka container:

```powershell
docker exec -it build-a-real-time-ride-surge-pricing-engine-with-kafka-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Describe the ride request topic:

```powershell
docker exec -it build-a-real-time-ride-surge-pricing-engine-with-kafka-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic ride-requests
```

Describe the surge alerts topic:

```powershell
docker exec -it build-a-real-time-ride-surge-pricing-engine-with-kafka-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic surge-alerts
```

Kafka topics should be verified explicitly during submission checks, especially when the assignment requires a fixed partition count such as 6 partitions for `ride-requests`.[1][9]

## Git Setup

Initialize Git and push the project to GitHub:

```powershell
git init
git branch -M main
git add .
git commit -m "Initial project commit"
git remote add origin https://github.com/YOUR-USERNAME/YOUR-REPO.git
git push -u origin main
```

Do not commit `.env`; keep it in `.gitignore` and publish only `.env.example` for setup instructions.[11][12]

Suggested `.gitignore`:

```gitignore
.env
__pycache__/
*.pyc
.venv/
```

## Submission Checklist

Before submitting, verify all of the following:

- `docker compose up --build -d` works from a clean start.
- `docker compose ps` shows Kafka and PostgreSQL healthy.[13]
- `GET /health` responds successfully.[8]
- `GET /api/zones/live` returns zone stats from PostgreSQL.[6]
- `GET /api/alerts/recent` returns recent alerts when surge conditions occur.[9]
- `ride-requests` has exactly 6 partitions.[1]
- `.env` is not committed to GitHub.[11][12]
- `README.md`, `docker-compose.yml`, `.env.example`, and all source folders are present in the repo.

## Troubleshooting

### Kafka fails to start

If Kafka reports an `advertised.listeners` error, ensure `KAFKA_ADVERTISED_LISTENERS` uses routable addresses like `kafka:29092` for internal clients and `localhost:9092` for host access, not `0.0.0.0`.[17][18]

### API is not reachable on port 8000

Ensure the API container starts Uvicorn with:

```dockerfile
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
```

Binding to `0.0.0.0` is required for Docker port publishing to work correctly with FastAPI/Uvicorn.[15][16]

### Check logs

Use service-specific logs to debug failures:

```powershell
docker compose logs --tail 100 kafka
docker compose logs --tail 100 api
docker compose logs --tail 100 producer
docker compose logs --tail 100 pricing-engine-consumer
docker compose logs --tail 100 analytics-recorder-consumer
```

Docker Compose logs are the standard way to inspect service-level startup and runtime problems in multi-container projects.[19][20]

## Notes

This project is designed for local development and demonstration. The current architecture is sufficient for a submission or portfolio project, while the same event-driven design could later be extended with better observability, schema validation, retry strategies, and deployment automation for more production-like usage.[10][21]
