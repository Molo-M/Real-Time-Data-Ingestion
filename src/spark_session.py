"""
spark_session.py
Creates the SparkSession used by the streaming job and check_setup.py,
so both run Spark with the same settings.
"""

import os
import sys
import urllib.request

import pyspark
from pyspark.sql import SparkSession

from src.config import (POSTGRES_JDBC_JAR, POSTGRES_JDBC_URL, SHUFFLE_PARTITIONS,
                        SPARK_APP_NAME, SPARK_MASTER)


def ensure_jdbc_driver():
    """Download the PostgreSQL JDBC driver into jars/ if it isn't there yet."""
    if POSTGRES_JDBC_JAR.exists():
        return
    print(f"Downloading the PostgreSQL JDBC driver to {POSTGRES_JDBC_JAR.parent} ...")
    POSTGRES_JDBC_JAR.parent.mkdir(parents=True, exist_ok=True)
    # Download to a temporary name first, so a failed download never leaves a broken jar
    temp_path = POSTGRES_JDBC_JAR.with_suffix(".part")
    urllib.request.urlretrieve(POSTGRES_JDBC_URL, temp_path)
    temp_path.replace(POSTGRES_JDBC_JAR)


def get_spark():
    """Create the SparkSession, or return the one that is already running."""
    # Spark runs Python code in separate worker processes. On Windows those
    # workers can start a different Python than the one running this code,
    # so point them at the Python that is running right now.
    os.environ["PYSPARK_PYTHON"] = sys.executable

    # Use the Spark that pip installed in this environment, so the version
    # always matches the pyspark package
    os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)

    ensure_jdbc_driver()

    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(SPARK_APP_NAME)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        # Give Spark the JDBC driver so it can write to PostgreSQL.
        # spark.jars (not spark.jars.packages) is used on purpose: packages
        # are also sent to the Python workers, which crashes them on Windows.
        # as_uri() writes the path in a form that handles spaces in folder names.
        .config("spark.jars", POSTGRES_JDBC_JAR.as_uri())
        .getOrCreate()
    )

    # Only show warnings and errors, not Spark's routine INFO messages
    spark.sparkContext.setLogLevel("WARN")

    # On Windows, Spark can't always delete its temporary files when it shuts
    # down, because Java still has them open. This is harmless, so these two
    # shutdown messages are hidden.
    jvm = spark.sparkContext._jvm
    configurator = jvm.org.apache.logging.log4j.core.config.Configurator
    level = jvm.org.apache.logging.log4j.Level
    configurator.setLevel("org.apache.spark.util.ShutdownHookManager", level.OFF)
    configurator.setLevel("org.apache.spark.SparkEnv", level.ERROR)

    # Two harmless warnings printed whenever a stream starts: one says a batch-only
    # optimization is switched off for streams, the other that a long internal
    # description of the query was shortened
    configurator.setLevel("org.apache.spark.sql.execution.streaming.runtime.ResolveWriteToStream", level.ERROR)
    configurator.setLevel("org.apache.spark.util.SparkStringUtils", level.ERROR)
    return spark