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

# ----------------------------------------------------------------------
# Spark settings
# ----------------------------------------------------------------------

SPARK_APP_NAME = "Ecommerce-Streaming-to-Postgres"

# Run Spark on this machine, using all CPU cores
SPARK_MASTER = "local[*]"

# Spark's default of 200 partitions after a shuffle is meant for large
# clusters. Each micro-batch here is small, so 4 is much faster.
SHUFFLE_PARTITIONS = 4

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