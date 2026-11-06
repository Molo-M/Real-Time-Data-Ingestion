"""
config.py
Settings used across the whole project: folders, Spark settings and the
PostgreSQL connection. Connection values are read from the .env file.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load the variables stored in the .env file
load_dotenv()

# ----------------------------------------------------------------------
# Folders
# ----------------------------------------------------------------------

# The project folder (one level above this src/ folder)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
INCOMING_DIR = DATA_DIR / "incoming"        # the generator writes CSVs here; Spark watches it
CHECKPOINT_DIR = DATA_DIR / "checkpoints"   # Spark's record of which files it has processed
METRICS_DIR = DATA_DIR / "metrics"          # per-batch performance logs
TEMP_DIR = DATA_DIR / "tmp"                 # the generator writes each file here first

# The stream's record of every micro-batch it wrote to PostgreSQL
BATCH_LOG = METRICS_DIR / "batch_log.csv"

# Spark's own progress report for every micro-batch (speed and time per stage)
PROGRESS_LOG = METRICS_DIR / "progress_log.csv"

# The generator's record of every file it wrote: how many valid, messy,
# invalid and duplicate rows each one contains (the answer key for testing)
GENERATOR_LOG = METRICS_DIR / "generator_log.csv"

# ----------------------------------------------------------------------
# Events
# ----------------------------------------------------------------------

# The columns of every CSV file, in order
CSV_COLUMNS = [
    "event_id", "event_time", "user_id", "session_id", "event_type",
    "product_id", "product_name", "category", "price", "quantity",
]

# The only event types the pipeline accepts (the events table enforces this too)
EVENT_TYPES = ["view", "add_to_cart", "purchase"]

# Format of event_time in the CSV files (millisecond precision for latency measurements).
# The first is Python's version (for the generator), the second Spark's (for parsing).
EVENT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
SPARK_TIME_FORMATS = ["yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss"]

# ----------------------------------------------------------------------
# Data generator defaults (each can be changed on the command line)
# ----------------------------------------------------------------------

EVENTS_PER_FILE = 50        # rows in each CSV file
FILE_INTERVAL_SECONDS = 5   # pause between files
INVALID_ROW_RATE = 0.05     # share of rows that break a rule (should be rejected)
DUPLICATE_ROW_RATE = 0.02   # share of rows that repeat a recent event (should be stored once)
MESSY_ROW_RATE = 0.05       # share of valid rows with messy formatting (should be cleaned and kept)

# ----------------------------------------------------------------------
# Spark settings
# ----------------------------------------------------------------------

SPARK_APP_NAME = "Ecommerce-Streaming-to-Postgres"

# Run Spark on this machine, using all CPU cores
SPARK_MASTER = "local[*]"

# Spark's default of 200 partitions after a shuffle is meant for large
# clusters. Each micro-batch here is small, so 4 is much faster.
SHUFFLE_PARTITIONS = 4

# How often the stream checks data/incoming/ for new files, and the most
# files it takes into one micro-batch (so a backlog is processed in steps)
TRIGGER_INTERVAL = "5 seconds"
MAX_FILES_PER_TRIGGER = 20

# Spark's progress record for the stream: which files it has already processed.
# Deleting this folder makes Spark treat every file as new again.
STREAM_CHECKPOINT = CHECKPOINT_DIR / "events_to_postgres"

# The PostgreSQL JDBC driver, which lets Spark talk to PostgreSQL.
# It's downloaded from Maven Central into the jars/ folder the first time
# Spark starts, and reused after that.
POSTGRES_JDBC_VERSION = "42.7.4"
JARS_DIR = PROJECT_ROOT / "jars"
POSTGRES_JDBC_JAR = JARS_DIR / f"postgresql-{POSTGRES_JDBC_VERSION}.jar"
POSTGRES_JDBC_URL = (
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/"
    f"{POSTGRES_JDBC_VERSION}/postgresql-{POSTGRES_JDBC_VERSION}.jar"
)

# ----------------------------------------------------------------------
# PostgreSQL connection (values come from .env)
# ----------------------------------------------------------------------

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "ecommerce_streaming")
PG_USER = os.getenv("PG_USER", "streaming_user")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# Spark connects through JDBC
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
JDBC_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
    # Spark sends text values as plain strings. This lets PostgreSQL convert
    # them to the column's own type, e.g. event_id text -> UUID column.
    "stringtype": "unspecified",
}

# Python connects directly with psycopg (used for SQL that Spark's JDBC
# writer can't run, such as the insert that skips duplicate events)
PSYCOPG_PARAMS = {
    "host": PG_HOST,
    "port": PG_PORT,
    "dbname": PG_DATABASE,
    "user": PG_USER,
    "password": PG_PASSWORD,
}

# Tables created by sql/postgres_setup.sql
EVENTS_TABLE = "events"
STAGING_TABLE = "events_staging"
REJECTED_TABLE = "rejected_events"