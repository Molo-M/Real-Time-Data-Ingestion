"""
metrics.py
Performance measurement.

ProgressLogger records Spark's progress report for every micro-batch in
data/metrics/progress_log.csv: how many rows it read, how fast, and how long
each stage took. latency_summary() measures, from the timestamps stored in
PostgreSQL, how long events took to travel through the pipeline.
"""

import csv

import psycopg
from pyspark.sql.streaming import StreamingQueryListener

from src.config import EVENTS_TABLE, PROGRESS_LOG, PSYCOPG_PARAMS

PROGRESS_COLUMNS = [
    "batch_id", "timestamp", "num_input_rows", "input_rows_per_second",
    "processed_rows_per_second", "trigger_ms", "add_batch_ms", "get_batch_ms",
    "latest_offset_ms", "query_planning_ms", "wal_commit_ms", "commit_offsets_ms",
]

# Spark's names for the stages of a micro-batch, and the log's names for them
STAGES = {
    "triggerExecution": "trigger_ms",      # the whole batch, start to finish
    "addBatch": "add_batch_ms",            # running write_batch(): cleaning + writing to PostgreSQL
    "getBatch": "get_batch_ms",            # preparing to read the new files
    "latestOffset": "latest_offset_ms",    # listing data/incoming/ to find new files
    "queryPlanning": "query_planning_ms",  # Spark planning how to run the batch
    "walCommit": "wal_commit_ms",          # recording which files the batch will read
    "commitOffsets": "commit_offsets_ms",  # recording that the batch is finished
}


class ProgressLogger(StreamingQueryListener):
    """Spark calls these methods as the stream starts, makes progress and stops.
    Only batches that read rows are logged: Spark also reports empty checks."""

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        progress = event.progress
        if progress.numInputRows == 0:
            return
        row = {
            "batch_id": progress.batchId,
            "timestamp": progress.timestamp,
            "num_input_rows": progress.numInputRows,
            "input_rows_per_second": round(progress.inputRowsPerSecond, 1),
            "processed_rows_per_second": round(progress.processedRowsPerSecond, 1),
        }
        for spark_name, log_name in STAGES.items():
            row[log_name] = progress.durationMs.get(spark_name, 0)
        append_row(PROGRESS_LOG, PROGRESS_COLUMNS, row)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


# Listeners belong to the Spark session, so each session gets only one
_sessions_with_logger = set()


def attach_progress_logger(spark):
    """Start recording progress for every stream in this Spark session."""
    session_id = id(spark)
    if session_id not in _sessions_with_logger:
        spark.streams.addListener(ProgressLogger())
        _sessions_with_logger.add(session_id)


def append_row(path, columns, row):
    """Add one row to a CSV log, writing the header first if the file is new."""
    path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        if is_new:
            writer.writeheader()
        writer.writerow(row)


def latency_summary(source_file_pattern=None):
    """End-to-end latency, from the timestamps stored with every event:
        event_time   when the user acted (written by the generator)
        ingested_at  when Spark processed the batch
        inserted_at  when the row arrived in the events table
    Returns seconds: average, median, 95th percentile and maximum, plus the
    average split into waiting/reading (event -> Spark) and writing (Spark -> table).
    source_file_pattern limits it to events from matching files, e.g. "events_2026%"."""
    where = "WHERE source_file LIKE %s" if source_file_pattern else ""
    sql = f"""
        WITH latency AS (
            SELECT EXTRACT(EPOCH FROM inserted_at - event_time) AS total,
                   EXTRACT(EPOCH FROM ingested_at - event_time) AS until_spark,
                   EXTRACT(EPOCH FROM inserted_at - ingested_at) AS writing
            FROM {EVENTS_TABLE} {where}
        )
        SELECT COUNT(*), AVG(total),
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total),
               PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total),
               MAX(total), AVG(until_spark), AVG(writing)
        FROM latency
    """
    with psycopg.connect(**PSYCOPG_PARAMS) as conn:
        row = conn.execute(sql, (source_file_pattern,) if source_file_pattern else None).fetchone()
    names = ["events", "avg_s", "median_s", "p95_s", "max_s", "avg_until_spark_s", "avg_writing_s"]
    return {name: (round(float(value), 2) if value is not None and name != "events" else value)
            for name, value in zip(names, row)}
