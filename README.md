# Ride Surge Pricing Engine (Kafka)

This project implements a simplified surge pricing engine using Apache Kafka, PostgreSQL, and containerized microservices.

## Quick start

1. Copy `.env.example` to `.env` and adjust values if needed.
2. Run:

   ```bash
   docker compose up --build -d
   ```

3. Verify services:

   ```bash
   docker compose ps
   ```

The stack includes Kafka, Zookeeper, PostgreSQL, a ride-request producer, a pricing-engine consumer, an analytics-recorder consumer, and a read-only REST API for zone stats and surge alerts.
