# Real-Time Retail Data Lakehouse

## Overview
This project simulates a full-stack data platform. It ingests fake retail transactions in real-time, processes them with Spark, stores them in an Iceberg Data Lake, and runs batch analytics using dbt and DuckDB.

## Architecture
Producer -> Redpanda (Kafka) -> Spark Streaming -> MinIO (S3) -> dbt -> Dagster

## How to Run
1. Start infrastructure: docker-compose up -d
2. Run producer: python producer.py
3. Run orchestrator: dagster dev -f orchestrator.py