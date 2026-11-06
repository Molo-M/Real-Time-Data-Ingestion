"""
monitor.py
Helpers for watching the running stream and checking its results.
Used by the notebook (and later for measuring performance).
"""

import csv
import time
from datetime import datetime

from src.config import BATCH_LOG, GENERATOR_LOG, INCOMING_DIR
from src.postgres_sink import table_counts


def read_log(path):
    """Read one of the CSV logs as a list of dictionaries (empty if it doesn't exist yet)."""
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def expected_totals():
    """The answer key: what the generator wrote, added up over all its files."""
    rows = read_log(GENERATOR_LOG)
    total = lambda column: sum(int(row[column]) for row in rows)
    return {
        "files": len(rows),
        "rows": total("total_rows"),
        "valid": total("valid_rows"),
        "messy": total("messy_rows"),
        "invalid": total("invalid_rows"),
        "duplicate": total("duplicate_rows"),
    }


def rows_processed():
    """How many CSV rows the stream has processed so far, from the batch log."""
    return sum(int(row["rows_read"]) for row in read_log(BATCH_LOG))


def wait_for_files(min_files=1, timeout=180):
    """Wait until the generator has written at least min_files files."""
    print(f"Waiting for at least {min_files} file(s) in {INCOMING_DIR.name}/ ...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        files = sorted(INCOMING_DIR.glob("*.csv")) if INCOMING_DIR.exists() else []
        if len(files) >= min_files:
            print(f"Found {len(files)} file(s).")
            return files
        time.sleep(1)
    raise TimeoutError(f"No files after {timeout}s. Is the generator running?")


def watch(query, seconds=30, every=5):
    """Print the stream's progress every few seconds, for a while."""
    print(f"{'time':<10}{'batch':>6}{'rows':>7}{'rows/s':>9}{'events':>9}{'rejected':>10}  status")
    end = time.time() + seconds
    while time.time() < end and query.isActive:
        progress = query.lastProgress or {}
        counts = table_counts()
        print(f"{datetime.now():%H:%M:%S}  {progress.get('batchId', '-'):>6}"
              f"{progress.get('numInputRows', 0):>7}{progress.get('processedRowsPerSecond', 0):>9.0f}"
              f"{counts['events']:>9}{counts['rejected_events']:>10}  {query.status['message']}")
        time.sleep(every)


def wait_until_caught_up(timeout=180, quiet=False):
    """Wait until the stream has processed every row the generator wrote.
    Stop the generator first, otherwise there is always more to catch up on."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        expected, done = expected_totals()["rows"], rows_processed()
        if expected > 0 and done >= expected:
            if not quiet:
                print(f"Caught up: {done} of {expected} rows processed.")
            return
        if not quiet:
            print(f"Processed {done} of {expected} rows ...")
        time.sleep(1 if quiet else 5)
    raise TimeoutError("The stream didn't catch up in time.")


def check_results():
    """Compare the tables with the answer key. Prints PASS or FAIL for each check."""
    expected = expected_totals()
    counts = table_counts()
    checks = [
        ("Valid events stored once each", counts["events"], expected["valid"]),
        ("Invalid rows rejected", counts["rejected_events"], expected["invalid"]),
        ("Staging table emptied after each batch", counts["events_staging"], 0),
    ]
    all_passed = True
    for name, actual, wanted in checks:
        passed = actual == wanted
        all_passed &= passed
        print(f"[{'PASS' if passed else 'FAIL'}] {name}: expected {wanted}, found {actual}")
    print(f"\n{expected['duplicate']} duplicate rows were skipped, and {expected['messy']} messy "
          "rows were cleaned and stored.")
    return all_passed