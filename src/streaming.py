"""
streaming.py
Part 2 and 3 combined: the streaming query that watches data/incoming/,
cleans each micro-batch and writes it to PostgreSQL.
Used by spark_streaming_to_postgres.py and by the notebook.
"""

import shutil

from pyspark.sql import functions as F

from src.config import (BATCH_LOG, GENERATOR_LOG, INCOMING_DIR, MAX_FILES_PER_TRIGGER, STREAM_CHECKPOINT,
                        TEMP_DIR, TRIGGER_INTERVAL)
from src.postgres_sink import empty_tables, write_batch
from src.schemas import RAW_EVENT_SCHEMA


def read_event_stream(spark):
    """A streaming DataFrame of the CSV files arriving in data/incoming/.
    Nothing is read yet: the stream only starts when start_stream() is called."""
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    return (
        spark.readStream
        .schema(RAW_EVENT_SCHEMA)
        .option("header", True)
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .option("pathGlobFilter", "*.csv")   # only CSV files
        .csv(str(INCOMING_DIR))
        # Record which file each row came from (Spark's built-in file metadata)
        .withColumn("source_file", F.col("_metadata.file_name"))
    )


def start_stream(spark, trigger_interval=TRIGGER_INTERVAL):
    """Start the stream in the background and return it (a StreamingQuery).
    The checkpoint folder is how Spark remembers which files it has already
    processed, so a restarted stream continues where it stopped."""
    return (
        read_event_stream(spark)
        .writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", str(STREAM_CHECKPOINT))
        .trigger(processingTime=trigger_interval)
        .queryName("events_to_postgres")
        .start()
    )


def reset_pipeline():
    """Start from scratch: empty the tables, and delete the incoming files,
    Spark's checkpoint and both logs, so the generator log only describes
    the files of the new run (it's the answer key for checking the results)."""
    empty_tables()
    for folder in (INCOMING_DIR, TEMP_DIR, STREAM_CHECKPOINT):
        shutil.rmtree(folder, ignore_errors=True)
    BATCH_LOG.unlink(missing_ok=True)
    GENERATOR_LOG.unlink(missing_ok=True)
