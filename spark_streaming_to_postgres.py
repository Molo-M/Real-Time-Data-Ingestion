"""
spark_streaming_to_postgres.py
Parts 2 and 3: the Spark Structured Streaming job.

Watches data/incoming/ for new CSV files written by data_generator.py. Every few
seconds, it reads the new files as one micro-batch, cleans and validates the
events, and writes them to PostgreSQL: valid events to the events table, and
invalid rows to rejected_events with the reason. It runs until you press Ctrl+C.

Usage:
    python spark_streaming_to_postgres.py           # continue from where it last stopped
    python spark_streaming_to_postgres.py --reset   # start fresh: empty tables, files and logs

Run the generator in a second terminal at the same time:
    python data_generator.py

Don't run this script and the notebook at the same time: both would use the
same checkpoint (Spark's record of processed files).
"""

import argparse
import logging
import time

from py4j.protocol import Py4JError

from src.config import INCOMING_DIR, TRIGGER_INTERVAL
from src.postgres_sink import table_counts
from src.spark_session import get_spark
from src.streaming import reset_pipeline, start_stream


def parse_args():
    parser = argparse.ArgumentParser(description="Stream CSV events into PostgreSQL with Spark.")
    parser.add_argument("--reset", action="store_true",
                        help="empty the tables and delete incoming files, checkpoint and logs first")
    return parser.parse_args()


def shutdown(spark, query):
    """Stop the stream and Spark, and return the stream's error if it failed.

    On Windows, Ctrl+C reaches every program in the terminal, including Spark's
    Java process, which may shut itself down before Python gets here. So each
    step is attempted, and "Spark has already stopped" is treated as fine."""
    # Py4J prints the "connection closed" error before the script handles it,
    # which makes a normal shutdown look like a crash, so hide its messages
    logging.disable(logging.CRITICAL)
    error = None
    for step in ("check for errors", "stop the stream", "stop Spark"):
        try:
            if step == "check for errors":
                error = query.exception()
            elif step == "stop the stream":
                query.stop()
            else:
                spark.stop()
        except Exception:
            pass  # Spark had already shut down
    return error


def main():
    args = parse_args()
    if args.reset:
        reset_pipeline()
        print("Reset: tables emptied, incoming files, checkpoint and logs deleted.")

    spark = get_spark()
    query = start_stream(spark)
    print(f"Streaming started. Watching {INCOMING_DIR} every {TRIGGER_INTERVAL}.")
    print("Press Ctrl+C to stop.\n")

    last_batch = None
    try:
        # Check on the stream every second. A short sleep loop (instead of
        # query.awaitTermination()) lets Ctrl+C stop it cleanly on Windows.
        while query.isActive:
            progress = query.lastProgress
            if progress and progress["batchId"] != last_batch and progress["numInputRows"] > 0:
                last_batch = progress["batchId"]
                duration = progress["durationMs"].get("triggerExecution", 0) / 1000
                print(f"Batch {last_batch}: {progress['numInputRows']} rows in {duration:.1f}s "
                      f"({progress.get('processedRowsPerSecond', 0):.0f} rows/s)")
            time.sleep(1)
    except (KeyboardInterrupt, Py4JError):
        # Py4JError: Spark's Java process stopped while Python was talking to it
        # (on Windows, Ctrl+C can reach Java first)
        print("\nStopping the stream...")

    error = shutdown(spark, query)
    if error:
        print(f"The stream failed: {error}")

    # Read with psycopg, which works even after Spark has stopped
    counts = table_counts()
    print(f"\nRows in PostgreSQL: {counts['events']} events, {counts['rejected_events']} rejected")


if __name__ == "__main__":
    main()