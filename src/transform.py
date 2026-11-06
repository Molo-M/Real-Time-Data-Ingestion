"""
transform.py
Part 2: Clean and validate the raw events.

The functions take a DataFrame of raw text values (as read from the CSV files)
and work the same on a normal DataFrame and on a streaming one, so they can be
tried out on a few files in the notebook before being used in the stream.

    clean_events(raw)  ->  every row, with cleaned values and a reject_reason
    valid_events(df)   ->  rows that passed every check, ready for the events table
    rejected_events(df)->  rows that failed, with their original values and the reason
"""

from pyspark.sql import functions as F

from src.config import CSV_COLUMNS, EVENT_TYPES, SPARK_TIME_FORMATS

# A valid event_id looks like 3f2a1b4c-89fa-4a68-8fb5-d27bbeb79919
UUID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

# Columns that only need extra spaces removed (event_id and event_type are
# also lowercased, so "PURCHASE" and "purchase" are treated as the same)
TRIMMED_COLUMNS = ["event_time", "user_id", "session_id", "product_id",
                   "product_name", "category", "price", "quantity"]
LOWERCASED_COLUMNS = ["event_id", "event_type"]


def normalize(raw):
    """Fix formatting: trim spaces, lowercase event_type and event_id,
    and turn empty text into null. Adds clean_* columns and keeps the originals."""
    df = raw
    for column in TRIMMED_COLUMNS + LOWERCASED_COLUMNS:
        value = F.trim(F.col(column))
        if column in LOWERCASED_COLUMNS:
            value = F.lower(value)
        df = df.withColumn(f"clean_{column}", F.when(value != "", value))
    return df


def parse_types(df):
    """Convert the clean text to real types. try_cast and try_to_timestamp give
    null for invalid values (e.g. quantity "two") instead of stopping the stream."""
    event_time = F.coalesce(*[F.try_to_timestamp(F.col("clean_event_time"), F.lit(fmt))
                              for fmt in SPARK_TIME_FORMATS])
    return (
        df
        .withColumn("parsed_event_time", event_time)
        .withColumn("parsed_user_id", F.col("clean_user_id").try_cast("int"))
        .withColumn("parsed_product_id", F.col("clean_product_id").try_cast("int"))
        .withColumn("parsed_price", F.col("clean_price").try_cast("decimal(10,2)"))
        .withColumn("parsed_quantity", F.col("clean_quantity").try_cast("int"))
    )


def add_reject_reason(df):
    """Check every rule and record the FIRST one a row breaks (null if it passes all).
    The rules match the constraints of the events table in PostgreSQL."""
    rules = [
        (F.col("clean_event_id").isNull(), "missing_event_id"),
        (~F.col("clean_event_id").rlike(UUID_PATTERN), "invalid_event_id"),
        (F.col("clean_event_time").isNull(), "missing_event_time"),
        (F.col("parsed_event_time").isNull(), "invalid_event_time"),
        (F.col("clean_user_id").isNull(), "missing_user_id"),
        (F.col("parsed_user_id").isNull(), "invalid_user_id"),
        (F.col("clean_session_id").isNull(), "missing_session_id"),
        (~F.col("clean_event_type").isin(EVENT_TYPES) | F.col("clean_event_type").isNull(),
         "unknown_event_type"),
        (F.col("parsed_product_id").isNull(), "invalid_product_id"),
        (F.col("clean_product_name").isNull(), "missing_product_name"),
        (F.col("clean_category").isNull(), "missing_category"),
        (F.col("parsed_price").isNull(), "invalid_price"),
        (F.col("parsed_price") <= 0, "non_positive_price"),
        (F.col("parsed_quantity").isNull(), "invalid_quantity"),
        (F.col("parsed_quantity") <= 0, "non_positive_quantity"),
    ]
    # Build "when rule 1 then reason 1, when rule 2 then reason 2, ..." so the
    # first broken rule wins
    reason = F.lit(None).cast("string")
    for condition, label in reversed(rules):
        reason = F.when(condition, label).otherwise(reason)
    return df.withColumn("reject_reason", reason)


def clean_events(raw):
    """Run all the cleaning steps. Every row is kept, with a reject_reason."""
    return add_reject_reason(parse_types(normalize(raw)))


def valid_events(df):
    """Rows that passed every check, in the shape of the events table."""
    return (
        df.filter(F.col("reject_reason").isNull())
        .select(
            F.col("clean_event_id").alias("event_id"),
            F.col("parsed_event_time").alias("event_time"),
            F.col("parsed_user_id").alias("user_id"),
            F.col("clean_session_id").alias("session_id"),
            F.col("clean_event_type").alias("event_type"),
            F.col("parsed_product_id").alias("product_id"),
            F.col("clean_product_name").alias("product_name"),
            F.col("clean_category").alias("category"),
            F.col("parsed_price").alias("price"),
            F.col("parsed_quantity").alias("quantity"),
            (F.col("parsed_price") * F.col("parsed_quantity")).cast("decimal(12,2)").alias("total_amount"),
            "source_file",
        )
    )


def rejected_events(df):
    """Rows that failed a check, with their ORIGINAL text values and the reason."""
    return (
        df.filter(F.col("reject_reason").isNotNull())
        .select(*CSV_COLUMNS, "reject_reason", "source_file")
    )
