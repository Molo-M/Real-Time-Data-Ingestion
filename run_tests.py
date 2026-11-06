"""
run_tests.py
Runs every test case in the test plan and records expected vs. actual results.

The tests cover the "What to Test" list from the project brief:
  - Are the CSV files being generated correctly?          TC01-TC03
  - Is Spark detecting and processing new files?          TC04-TC05
  - Are the data transformations correct?                 TC06-TC09
  - Is data being written into PostgreSQL without errors? TC10-TC12
  - Does the pipeline recover from stops and retries?     TC13-TC14
  - Are performance metrics within expected limits?       TC15-TC16

Results are printed and saved to data/metrics/test_results.md, ready to copy
into docs/test_cases.md.

WARNING: this resets the pipeline (empties the tables and deletes the data files).

Usage:
    python run_tests.py      # takes about 2-3 minutes
"""

import csv
import time
from collections import Counter

import psycopg

from data_generator import EventGenerator, log_file, write_csv_atomically
from src import monitor
from src.config import (CSV_COLUMNS, INCOMING_DIR, METRICS_DIR, PROGRESS_LOG, PSYCOPG_PARAMS,
                        TEMP_DIR, TRIGGER_INTERVAL)
from src.metrics import latency_summary
from src.postgres_sink import table_counts, write_batch
from src.schemas import RAW_EVENT_SCHEMA
from src.spark_session import get_spark
from src.streaming import reset_pipeline, start_stream

RESULTS_FILE = METRICS_DIR / "test_results.md"
ROWS_PER_FILE = 100

# Performance limits (with the default 5-second trigger)
MAX_BATCH_SECONDS = 5.0     # a batch should finish before the next trigger is due
MAX_P95_LATENCY_SECONDS = 15.0

results = []


def record(test_id, name, expected, actual, passed):
    """Save one test result and print it."""
    results.append((test_id, name, expected, actual, "PASS" if passed else "FAIL"))
    print(f"[{'PASS' if passed else 'FAIL'}] {test_id} {name}: {actual}")


def query_one(sql, params=None):
    """Run a SQL query and return the first row."""
    with psycopg.connect(**PSYCOPG_PARAMS) as conn:
        return conn.execute(sql, params).fetchone()


def query_all(sql, params=None):
    """Run a SQL query and return all rows."""
    with psycopg.connect(**PSYCOPG_PARAMS) as conn:
        return conn.execute(sql, params).fetchall()


def write_generated_files(generator, run_id, count):
    """Write files the way data_generator.py does, and log them (the answer key)."""
    names = []
    for number in range(1, count + 1):
        rows, counts = generator.make_rows(ROWS_PER_FILE, 0.05, 0.02, 0.05)
        name = f"events_{run_id}_{number:05d}.csv"
        write_csv_atomically(rows, name)
        log_file(run_id, name, counts, len(rows))
        names.append(name)
    return names


# ----------------------------------------------------------------------
# A hand-made file where the correct outcome of every row is known
# ----------------------------------------------------------------------

CONTROLLED_FILE = "events_controlled_test.csv"
VALID_ID = "11111111-1111-4111-8111-111111111111"
MESSY_ID = "22222222-2222-4222-8222-222222222222"


def controlled_rows():
    """2 valid rows (one messy), 1 duplicate, and 11 rows that each break one rule."""
    def row(number, **changes):
        base = {
            "event_id": f"{number:08d}-0000-4000-8000-000000000000", "event_time": "2026-11-06 10:00:00.000",
            "user_id": "1234", "session_id": "test-session", "event_type": "view", "product_id": "101",
            "product_name": "Wireless Headphones", "category": "Electronics", "price": "89.99", "quantity": "1",
        }
        base.update(changes)
        return base

    return [
        (row(1, event_id=VALID_ID), None),
        (row(2, event_id=MESSY_ID, event_type=" PURCHASE ", product_name="  Desk Lamp ", quantity="2"), None),
        (row(3, event_id=VALID_ID), "duplicate"),
        (row(4, user_id=""), "missing_user_id"),
        (row(5, user_id="12x"), "invalid_user_id"),
        (row(6, price="-5.00"), "non_positive_price"),
        (row(7, price="abc"), "invalid_price"),
        (row(8, quantity="0"), "non_positive_quantity"),
        (row(9, quantity="two"), "invalid_quantity"),
        (row(10, quantity="1.5"), "invalid_quantity"),
        (row(11, event_time="not-a-date"), "invalid_event_time"),
        (row(12, event_type="refund"), "unknown_event_type"),
        (row(13, event_id="abc"), "invalid_event_id"),
        (row(14, product_name=""), "missing_product_name"),
    ]


def write_controlled_file():
    rows = controlled_rows()
    write_csv_atomically([r for r, _ in rows], CONTROLLED_FILE)
    reasons = [reason for _, reason in rows if reason not in (None, "duplicate")]
    counts = {"valid": 2, "messy": 1, "invalid": len(reasons), "duplicate": 1}
    log_file("controlled", CONTROLLED_FILE, counts, len(rows))
    return Counter(reasons)


# ----------------------------------------------------------------------
# The tests
# ----------------------------------------------------------------------

def test_generator(files):
    """TC01-TC03: the CSV files are generated correctly."""
    headers_ok, row_counts = True, []
    for name in files:
        with open(INCOMING_DIR / name, newline="", encoding="utf-8") as file:
            reader = csv.reader(file)
            headers_ok &= next(reader) == CSV_COLUMNS
            rows = list(reader)
            headers_ok &= all(len(r) == len(CSV_COLUMNS) for r in rows)
            row_counts.append(len(rows))
    record("TC01", "Generated files have the correct columns and row count",
           f"{len(files)} files, each with the 10 columns and {ROWS_PER_FILE} rows",
           f"{len(files)} files, columns correct: {headers_ok}, rows per file: {sorted(set(row_counts))}",
           headers_ok and set(row_counts) == {ROWS_PER_FILE})

    leftovers = list(TEMP_DIR.glob("*")) if TEMP_DIR.exists() else []
    record("TC02", "Files are written atomically, with unique names",
           "data/tmp/ empty after writing; no repeated file names",
           f"{len(leftovers)} file(s) left in data/tmp/, {len(files) - len(set(files))} repeated name(s)",
           not leftovers and len(set(files)) == len(files))

    expected = monitor.expected_totals()
    parts = expected["valid"] + expected["invalid"] + expected["duplicate"]
    record("TC03", "Generator log matches the files written",
           "log rows = rows in files = valid + invalid + duplicate",
           f"log {expected['rows']}, files {sum(row_counts)}, parts {parts}",
           expected["rows"] == sum(row_counts) == parts)


def test_detection(query):
    """TC04-TC05: Spark detects and processes new files."""
    monitor.wait_until_caught_up(quiet=True)
    expected = monitor.expected_totals()["rows"]
    record("TC04", "Files already waiting when the stream starts are processed",
           f"{expected} rows processed", f"{monitor.rows_processed()} rows processed",
           monitor.rows_processed() == expected)

    before = monitor.rows_processed()
    write_generated_files(EventGenerator(seed=7), "while_running", 2)
    started = time.time()
    monitor.wait_until_caught_up(quiet=True)
    new_rows = monitor.rows_processed() - before
    record("TC05", "Files arriving while the stream runs are detected",
           f"{2 * ROWS_PER_FILE} new rows processed within 2 triggers",
           f"{new_rows} new rows processed after {time.time() - started:.1f}s",
           new_rows == 2 * ROWS_PER_FILE and query.isActive)


def test_transformations(expected_reasons):
    """TC06-TC09: the data transformations are correct."""
    monitor.wait_until_caught_up(quiet=True)
    badly_formatted = query_one(
        "SELECT COUNT(*) FROM events WHERE event_type <> lower(trim(event_type)) "
        "OR product_name <> trim(product_name)")[0]
    messy = query_one("SELECT event_type, product_name FROM events WHERE event_id = %s", (MESSY_ID,))
    record("TC06", "Messy rows are cleaned and kept",
           "' PURCHASE ' -> 'purchase', '  Desk Lamp ' -> 'Desk Lamp'; 0 badly formatted rows stored",
           f"stored as {messy}; {badly_formatted} badly formatted rows",
           messy == ("purchase", "Desk Lamp") and badly_formatted == 0)

    actual_reasons = Counter(dict(query_all(
        "SELECT reject_reason, COUNT(*) FROM rejected_events WHERE source_file = %s GROUP BY 1",
        (CONTROLLED_FILE,))))
    record("TC07", "Each kind of invalid row is rejected with the right reason",
           f"{sum(expected_reasons.values())} rows: " + ", ".join(f"{k} x{v}" for k, v in sorted(expected_reasons.items())),
           f"{sum(actual_reasons.values())} rows, all reasons match: {actual_reasons == expected_reasons}",
           actual_reasons == expected_reasons)

    from_controlled = query_one("SELECT COUNT(*) FROM events WHERE source_file = %s", (CONTROLLED_FILE,))[0]
    record("TC08", "A duplicate within the same file is stored once",
           "2 events stored from the controlled file (the duplicate skipped)",
           f"{from_controlled} events stored", from_controlled == 2)

    wrong_totals = query_one("SELECT COUNT(*) FROM events WHERE total_amount <> price * quantity")[0]
    messy_total = query_one("SELECT total_amount FROM events WHERE event_id = %s", (MESSY_ID,))[0]
    record("TC09", "total_amount is calculated correctly",
           "every total_amount = price x quantity (e.g. 89.99 x 2 = 179.98)",
           f"{wrong_totals} wrong totals; example: {messy_total}",
           wrong_totals == 0 and float(messy_total) == 179.98)


def test_postgres(query):
    """TC10-TC12: data is written to PostgreSQL without errors."""
    monitor.wait_until_caught_up(quiet=True)
    expected, counts = monitor.expected_totals(), table_counts()
    unique = query_one("SELECT COUNT(DISTINCT event_id) FROM events")[0]
    record("TC10", "Every valid event is stored exactly once",
           f"{expected['valid']} events (all unique)", f"{counts['events']} events, {unique} unique",
           counts["events"] == expected["valid"] == unique)
    record("TC11", "Every invalid row is stored in rejected_events",
           f"{expected['invalid']} rejected rows", f"{counts['rejected_events']} rejected rows",
           counts["rejected_events"] == expected["invalid"])
    error = query.exception()
    record("TC12", "No stream errors, and the staging table is emptied after each batch",
           "no exception; 0 rows in events_staging",
           f"exception: {error}; {counts['events_staging']} rows in events_staging",
           error is None and counts["events_staging"] == 0)


def test_recovery(spark, query):
    """TC13-TC14: the pipeline recovers from a restart and from a retried batch."""
    query.stop()
    write_generated_files(EventGenerator(seed=11), "while_stopped", 1)
    processed_before = monitor.rows_processed()
    query = start_stream(spark)
    monitor.wait_until_caught_up(quiet=True)
    new_rows = monitor.rows_processed() - processed_before
    record("TC13", "After a restart, only new files are processed",
           f"{ROWS_PER_FILE} new rows processed, nothing reprocessed",
           f"{new_rows} new rows processed; events match answer key: "
           f"{table_counts()['events'] == monitor.expected_totals()['valid']}",
           new_rows == ROWS_PER_FILE and table_counts()["events"] == monitor.expected_totals()["valid"])

    # Re-run a whole finished batch with its own batch number, as Spark does after a failure
    batch_id = query_one("SELECT MAX(batch_id) FROM events")[0]
    files = [r[0] for r in query_all(
        "SELECT source_file FROM events WHERE batch_id = %s UNION "
        "SELECT source_file FROM rejected_events WHERE batch_id = %s", (batch_id, batch_id))]
    batch = (spark.read.schema(RAW_EVENT_SCHEMA).option("header", True)
             .csv([str(INCOMING_DIR / name) for name in files])
             .selectExpr("*", "_metadata.file_name AS source_file"))
    before = table_counts()
    write_batch(batch, batch_id)
    after = table_counts()
    record("TC14", "A retried batch doesn't duplicate anything",
           f"same counts after re-running batch {batch_id}: {before['events']} events, {before['rejected_events']} rejected",
           f"{after['events']} events, {after['rejected_events']} rejected",
           before == after)
    return query


def test_performance():
    """TC15-TC16: performance is within the expected limits."""
    progress = monitor.read_log(PROGRESS_LOG)
    # The first batch includes one-off start-up work (loading the JDBC driver,
    # opening connections), so the limit applies to the batches after it
    durations = [int(row["trigger_ms"]) / 1000 for row in progress[1:]]
    slowest = max(durations) if durations else 0
    record("TC15", "Batches finish within the trigger interval (after the first)",
           f"every batch under {MAX_BATCH_SECONDS:.0f}s (trigger: {TRIGGER_INTERVAL})",
           f"{len(durations)} batches, average {sum(durations) / max(len(durations), 1):.1f}s, slowest {slowest:.1f}s",
           bool(durations) and slowest < MAX_BATCH_SECONDS)

    # Only events written while the stream was running: files waiting before it
    # started (or written while it was stopped) include that waiting time
    latency = latency_summary("events_while_running%")
    record("TC16", "95% of events reach PostgreSQL within the latency limit",
           f"95th percentile latency under {MAX_P95_LATENCY_SECONDS:.0f}s (events written while the stream runs)",
           f"95th percentile {latency['p95_s']}s (median {latency['median_s']}s, max {latency['max_s']}s)",
           latency["p95_s"] is not None and latency["p95_s"] < MAX_P95_LATENCY_SECONDS)


def save_results():
    lines = ["| ID | Test | Expected | Actual | Result |", "|---|---|---|---|---|"]
    for test_id, name, expected, actual, result in results:
        lines.append(f"| {test_id} | {name} | {expected} | {actual} | **{result}** |")
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    print("Resetting the pipeline (tables, data files, checkpoint and logs)...\n")
    reset_pipeline()

    # Part 1 runs without Spark: files waiting before the stream starts
    files = write_generated_files(EventGenerator(seed=2026), "before_start", 4)
    test_generator(files)
    expected_reasons = write_controlled_file()

    spark = get_spark()
    query = start_stream(spark)
    try:
        test_detection(query)
        test_transformations(expected_reasons)
        test_postgres(query)
        query = test_recovery(spark, query)
        test_performance()
    finally:
        query.stop()
        spark.stop()

    save_results()
    passed = sum(1 for r in results if r[4] == "PASS")
    print(f"\n{passed} of {len(results)} tests passed. Results saved to {RESULTS_FILE}")


if __name__ == "__main__":
    main()
