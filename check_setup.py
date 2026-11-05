"""
check_setup.py
Checks that everything the project needs works on this machine:
Spark, Python workers, writing files, PostgreSQL, and Spark's connection
to PostgreSQL. Each check runs on its own, so one failure doesn't hide others.

Usage:
    python check_setup.py
"""

import shutil

import psycopg

from src.config import (DATA_DIR, EVENTS_TABLE, JDBC_PROPERTIES, JDBC_URL, PG_PASSWORD,
                        PSYCOPG_PARAMS, REJECTED_TABLE, STAGING_TABLE)
from src.spark_session import get_spark


def check_python_workers(spark):
    """Python workers run Python code inside Spark (fails if Spark can't find the right Python)."""
    df = spark.createDataFrame([("view",), ("purchase",)], ["event_type"])
    result = df.rdd.map(lambda row: row.event_type.upper()).collect()
    return f"ran Python code in Spark: {result}"


def check_file_writing(spark):
    """Writing files, which streaming checkpoints rely on (needs winutils/hadoop.dll on Windows)."""
    path = DATA_DIR / "spark_write_test"
    spark.range(3).write.mode("overwrite").parquet(str(path))
    count = spark.read.parquet(str(path)).count()
    shutil.rmtree(path, ignore_errors=True)
    return f"wrote and read back {count} rows"


def check_postgres():
    """PostgreSQL is running, the login works, and the project tables exist."""
    if not PG_PASSWORD:
        raise ValueError("PG_PASSWORD is missing from your .env file")
    with psycopg.connect(**PSYCOPG_PARAMS, connect_timeout=5) as conn:
        version = conn.execute("SHOW server_version").fetchone()[0]
        tables = {row[0] for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")}
    missing = {EVENTS_TABLE, STAGING_TABLE, REJECTED_TABLE} - tables
    if missing:
        raise ValueError(f"tables missing: {sorted(missing)}. Run sql/postgres_setup.sql first")
    return f"PostgreSQL {version}, all 3 tables found"


def check_spark_to_postgres(spark):
    """Spark can download the JDBC driver and query PostgreSQL through it."""
    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("query", "SELECT current_database() AS db, current_user AS username")
        .options(**JDBC_PROPERTIES)
        .load()
    )
    row = df.first()
    return f"connected to database '{row.db}' as '{row.username}'"


def run(name, check, *args):
    """Run one check and report whether it passed."""
    try:
        detail = check(*args)
        print(f"[PASS] {name}: {detail}")
        return True
    except Exception as error:
        # Spark errors can be hundreds of lines long, so show only the first
        # meaningful line (skipping Py4J's generic "An error occurred..." line)
        lines = [line.strip(" :") for line in str(error).splitlines() if line.strip(" :")]
        lines = [line for line in lines if not line.startswith("An error occurred while calling")]
        message = lines[0] if lines else repr(error)
        print(f"[FAIL] {name}: {message[:300]}")
        return False


def main():
    results = []
    results.append(run("PostgreSQL", check_postgres))

    try:
        spark = get_spark()
        print(f"[PASS] Spark: version {spark.version} started")
        results.append(True)
    except Exception as error:
        print(f"[FAIL] Spark: {str(error).splitlines()[0]}")
        print("\nSpark didn't start, so the Spark checks were skipped. If the error mentions a")
        print("download, the first run needs an internet connection to fetch the PostgreSQL")
        print("JDBC driver into the jars/ folder.")
        return

    results.append(run("Python workers", check_python_workers, spark))
    results.append(run("File writing", check_file_writing, spark))
    results.append(run("Spark to PostgreSQL", check_spark_to_postgres, spark))
    spark.stop()

    failed = results.count(False)
    print("\nAll checks passed!" if failed == 0 else f"\n{failed} check(s) failed. See the messages above.")


if __name__ == "__main__":
    main()