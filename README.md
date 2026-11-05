# Real-Time Data Ingestion Using Spark Structured Streaming & PostgreSQL

## Project Overview

In this project, you'll build a real-time data pipeline that simulates an e-commerce platform tracking user activity. You'll generate fake user events (like product views and purchases), stream them using Apache Spark Structured Streaming, and store the processed data in a PostgreSQL database.

## Proposed Project Structure

```
Spark-Ecommerce-Streaming/
├── data/
│   ├── incoming/                     # generator writes CSVs here; Spark watches it
│   ├── checkpoints/                  # Spark's record of processed files (not committed)
│   └── metrics/                      # per-batch performance logs (for performance_metrics.md)
├── src/
│   ├── __init__.py
│   ├── config.py                     # folders, event types, rates, Postgres settings from .env
│   ├── spark_session.py              # reused from TMDB, plus the Postgres JDBC driver
│   ├── schemas.py                    # schema of the CSV events
│   ├── transform.py                  # cleaning/validation functions (testable on their own)
│   └── metrics.py                    # listener that logs each batch's throughput and duration
├── sql/
│   ├── postgres_setup.sql            # DELIVERABLE: creates database, user and tables
│   └── verification_queries.sql      # queries to check the data and measure latency
├── docs/
│   ├── project_overview.md           # DELIVERABLE
│   ├── user_guide.md                 # DELIVERABLE
│   ├── test_cases.md                 # DELIVERABLE
│   ├── performance_metrics.md        # DELIVERABLE
│   └── system_architecture.png       # DELIVERABLE
├── data_generator.py                 # DELIVERABLE: Part 1
├── spark_streaming_to_postgres.py    # DELIVERABLE: Parts 2 and 3
├── postgres_connection_details.txt   # DELIVERABLE: host, port, user, password (local only)
├── check_setup.py                    # reused, plus a Postgres connection check
├── docker-compose.yml                # runs Postgres (if we use Docker, see below)
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```