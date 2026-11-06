"""
postgres_sink.py
Part 3: Write each micro-batch to PostgreSQL.

Spark has no built-in streaming connection to PostgreSQL, so the stream calls
write_batch() once per micro-batch (Spark's foreachBatch). Inside it, the batch
is a normal DataFrame and can be written with Spark's JDBC writer.

Writing is safe to repeat: if Spark retries a batch after a failure, no event
is stored twice and no rejected row is logged twice.
"""

import csv
import time
from datetime import datetime

import psycopg
from pyspark.sql import functions as F

from src.config import (BATCH_LOG, EVENTS_TABLE, JDBC_PROPERTIES, JDBC_URL, PSYCOPG_PARAMS,
                        REJECTED_TABLE, STAGING_TABLE)
from src.transform import clean_events, rejected_events, valid_events

EVENT_COLUMNS = [
    "event_id", "event_time", "user_id", "session_id", "event_type", "product_id",
    "product_name", "category", "price", "quantity", "total_amount", "source_file",
    "batch_id", "ingested_at",
]

# Copies the batch from the staging table into events, skipping any event_id
# that is already stored (from an earlier batch, or a retried one)
MERGE_SQL = f"""
    INSERT INTO {EVENTS_TABLE} ({", ".join(EVENT_COLUMNS)})
    SELECT {", ".join(EVENT_COLUMNS)} FROM {STAGING_TABLE}
    ON CONFLICT (event_id) DO NOTHING
"""

BATCH_LOG_COLUMNS = [
    "batch_id", "finished_at", "duration_seconds", "rows_read", "valid_rows",
    "duplicates_in_batch", "inserted_rows", "already_stored", "rejected_rows",
]


def add_batch_columns(df, batch_id):
    """Stamp rows with the batch number and the time Spark processed them."""
    return (
        df.withColumn("batch_id", F.lit(batch_id).cast("long"))
        .withColumn("ingested_at", F.current_timestamp())
    )


def write_jdbc(df, table):
    """Append a DataFrame to a PostgreSQL table through Spark's JDBC writer."""
    df.write.jdbc(JDBC_URL, table, mode="append", properties=JDBC_PROPERTIES)


def write_batch(batch_df, batch_id):
    """Clean one micro-batch and write it to PostgreSQL.
    Called by Spark for every micro-batch, with the batch's rows and its number."""
    started = time.perf_counter()

    # The batch is used several times below (counts, valid rows, rejected rows),
    # so keep it in memory instead of re-reading the CSV files each time
    cleaned = clean_events(batch_df).persist()
    try:
        rows_read = cleaned.count()
        valid = add_batch_columns(valid_events(cleaned), batch_id)
        valid_count = valid.count()
        # Duplicates inside this batch are removed here; duplicates of events
        # stored by earlier batches are skipped by the merge (ON CONFLICT)
        unique_valid = valid.dropDuplicates(["event_id"])
        unique_count = unique_valid.count()
        rejected = add_batch_columns(rejected_events(cleaned), batch_id)
        rejected_count = rejected.count()

        with psycopg.connect(**PSYCOPG_PARAMS) as conn:
            # Empty the staging table, and remove rejected rows from an earlier
            # attempt at this same batch (in case Spark is retrying it)
            conn.execute(f"TRUNCATE {STAGING_TABLE}")
            conn.execute(f"DELETE FROM {REJECTED_TABLE} WHERE batch_id = %s", (batch_id,))

        write_jdbc(unique_valid.select(EVENT_COLUMNS), STAGING_TABLE)
        write_jdbc(rejected, REJECTED_TABLE)

        with psycopg.connect(**PSYCOPG_PARAMS) as conn:
            inserted = conn.execute(MERGE_SQL).rowcount
            conn.execute(f"TRUNCATE {STAGING_TABLE}")
    finally:
        cleaned.unpersist()

    stats = {
        "batch_id": batch_id,
        "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "duration_seconds": round(time.perf_counter() - started, 3),
        "rows_read": rows_read,
        "valid_rows": valid_count,
        "duplicates_in_batch": valid_count - unique_count,
        "inserted_rows": inserted,
        "already_stored": unique_count - inserted,
        "rejected_rows": rejected_count,
    }
    log_batch(stats)
    return stats


def log_batch(stats):
    """Add one line per batch to data/metrics/batch_log.csv."""
    BATCH_LOG.parent.mkdir(parents=True, exist_ok=True)
    is_new = not BATCH_LOG.exists()
    with open(BATCH_LOG, "a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=BATCH_LOG_COLUMNS)
        if is_new:
            writer.writeheader()
        writer.writerow(stats)


def table_counts():
    """How many rows each table holds (used to check the pipeline's results)."""
    with psycopg.connect(**PSYCOPG_PARAMS) as conn:
        return {table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in (EVENTS_TABLE, REJECTED_TABLE, STAGING_TABLE)}


def empty_tables():
    """Delete every row from the project's tables, for a fresh test run."""
    with psycopg.connect(**PSYCOPG_PARAMS) as conn:
        conn.execute(f"TRUNCATE {EVENTS_TABLE}, {STAGING_TABLE}, {REJECTED_TABLE} RESTART IDENTITY")


def read_query(spark, sql):
    """Run a SQL query on PostgreSQL through Spark and return the result as a DataFrame."""
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("query", sql)
        .options(**JDBC_PROPERTIES)
        .load()
    )
