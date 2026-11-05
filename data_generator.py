"""
data_generator.py
Part 1: Simulate an e-commerce website by writing fake user events to CSV files.

Every few seconds, it writes a new CSV file of events (product views, add-to-carts
and purchases) into data/incoming/, which the Spark streaming job watches.

On purpose, a small share of rows are imperfect, to test the cleaning step:
  - invalid rows break a rule (e.g. negative price) and should be REJECTED
  - duplicate rows repeat a recent event and should be STORED ONCE
  - messy rows are valid but badly formatted (e.g. " PURCHASE ") and should be CLEANED and KEPT
Every file is recorded in data/metrics/generator_log.csv with its counts of each kind.

Usage:
    python data_generator.py                           # 50 events every 5 seconds, until Ctrl+C
    python data_generator.py --files 10                # stop after 10 files
    python data_generator.py --events-per-file 500 --interval 1
    python data_generator.py --invalid-rate 0 --duplicate-rate 0 --messy-rate 0   # clean data only
    python data_generator.py --seed 42                 # the same "random" events every run
"""

import argparse
import csv
import random
import time
import uuid
from collections import deque
from datetime import datetime

from src.config import (CSV_COLUMNS, DUPLICATE_ROW_RATE, EVENT_TIME_FORMAT, EVENTS_PER_FILE,
                        FILE_INTERVAL_SECONDS, GENERATOR_LOG, INCOMING_DIR, INVALID_ROW_RATE,
                        MESSY_ROW_RATE, TEMP_DIR)

# ----------------------------------------------------------------------
# The shop: a fixed product catalog, so a product always has the same
# name, category and price (product_id, product_name, category, price)
# ----------------------------------------------------------------------

PRODUCTS = [
    (101, "Wireless Headphones", "Electronics", 89.99),
    (102, "Smartphone Case", "Electronics", 19.99),
    (103, "USB-C Charger", "Electronics", 24.50),
    (104, "Bluetooth Speaker", "Electronics", 59.00),
    (105, "Smart Watch", "Electronics", 199.99),
    (201, "Running Shoes", "Fashion", 120.00),
    (202, "Denim Jacket", "Fashion", 75.50),
    (203, "Cotton T-Shirt", "Fashion", 15.99),
    (204, "Leather Wallet", "Fashion", 35.00),
    (301, "Coffee Maker", "Home & Kitchen", 49.99),
    (302, "Non-Stick Frying Pan", "Home & Kitchen", 29.99),
    (303, "Scented Candle", "Home & Kitchen", 12.50),
    (304, "Desk Lamp", "Home & Kitchen", 22.00),
    (401, "Yoga Mat", "Sports", 25.00),
    (402, "Water Bottle", "Sports", 14.99),
    (403, "Dumbbell Set", "Sports", 89.00),
    (501, "Mystery Novel", "Books", 11.99),
    (502, "Cookbook", "Books", 24.99),
    (503, "Children's Picture Book", "Books", 8.99),
    (504, "Data Engineering Guide", "Books", 39.99),
]

# Like a real shop: many views, fewer add-to-carts, even fewer purchases
EVENT_TYPE_WEIGHTS = {"view": 70, "add_to_cart": 20, "purchase": 10}

# How many shoppers are browsing at the same time, and the chance that a
# session ends after each event (a new shopper then takes its place)
ACTIVE_SESSIONS = 25
SESSION_END_CHANCE = 0.05

# The ways an invalid row can break a rule (the cleaning step must catch each one)
INVALID_KINDS = [
    "missing_user_id", "negative_price", "unknown_event_type",
    "bad_timestamp", "non_numeric_quantity",
]


class EventGenerator:
    """Creates fake events. A random generator with a fixed seed gives the same
    events every run, which makes test results repeatable."""

    def __init__(self, seed=None):
        self.rng = random.Random(seed)
        self.sessions = [self._new_session() for _ in range(ACTIVE_SESSIONS)]
        # Recent valid events, which duplicates are copied from. Some will be
        # from the previous file, so duplicates can also cross file boundaries.
        self.recent_events = deque(maxlen=200)

    def _new_session(self):
        """A shopper: a user id and a session id for this visit."""
        return {"user_id": self.rng.randint(1000, 9999), "session_id": self._uuid()}

    def _uuid(self):
        """A random id. Built from self.rng so that --seed also repeats the ids."""
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def valid_event(self):
        """One correct event, as a dictionary of CSV values."""
        index = self.rng.randrange(len(self.sessions))
        session = self.sessions[index]
        if self.rng.random() < SESSION_END_CHANCE:
            self.sessions[index] = self._new_session()

        product_id, name, category, price = self.rng.choice(PRODUCTS)
        event_type = self.rng.choices(list(EVENT_TYPE_WEIGHTS), weights=EVENT_TYPE_WEIGHTS.values())[0]
        # A view is always of one item; carts and purchases can have up to 3
        quantity = 1 if event_type == "view" else self.rng.randint(1, 3)

        return {
            "event_id": self._uuid(),
            "event_time": datetime.now().strftime(EVENT_TIME_FORMAT)[:-3],  # milliseconds
            "user_id": str(session["user_id"]),
            "session_id": session["session_id"],
            "event_type": event_type,
            "product_id": str(product_id),
            "product_name": name,
            "category": category,
            "price": f"{price:.2f}",
            "quantity": str(quantity),
        }

    def make_messy(self, event):
        """Valid but badly formatted: random capitals and extra spaces.
        The cleaning step should fix it and keep the row."""
        event = dict(event)
        event["event_type"] = self.rng.choice([
            event["event_type"].upper(), f"  {event['event_type']} ", event["event_type"].title(),
        ])
        event["product_name"] = f" {event['product_name']}  "
        return event

    def make_invalid(self, event):
        """Breaks one rule, so the cleaning step should reject the row.
        Returns the row and which rule it breaks."""
        event = dict(event)
        kind = self.rng.choice(INVALID_KINDS)
        if kind == "missing_user_id":
            event["user_id"] = ""
        elif kind == "negative_price":
            event["price"] = f"-{event['price']}"
        elif kind == "unknown_event_type":
            event["event_type"] = self.rng.choice(["click", "wishlist", "refund"])
        elif kind == "bad_timestamp":
            event["event_time"] = "not-a-date"
        elif kind == "non_numeric_quantity":
            event["quantity"] = self.rng.choice(["two", "many", "1.5"])
        return event, kind

    def make_rows(self, count, invalid_rate, duplicate_rate, messy_rate):
        """Build the rows for one file. Returns the rows and a count of each kind."""
        rows = []
        counts = {"valid": 0, "messy": 0, "invalid": 0, "duplicate": 0}
        for _ in range(count):
            roll = self.rng.random()
            if roll < invalid_rate:
                row, _ = self.make_invalid(self.valid_event())
                counts["invalid"] += 1
            elif roll < invalid_rate + duplicate_rate and self.recent_events:
                row = dict(self.rng.choice(self.recent_events))
                counts["duplicate"] += 1
            else:
                row = self.valid_event()
                self.recent_events.append(row)
                counts["valid"] += 1
                if self.rng.random() < messy_rate:
                    row = self.make_messy(row)
                    counts["messy"] += 1   # messy rows are also counted as valid
            rows.append(row)
        return rows, counts


def write_csv_atomically(rows, file_name):
    """Write the file in data/tmp/ first, then move it into data/incoming/ in one step.
    Spark watches data/incoming/, so it never sees a half-written file."""
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    temp_path = TEMP_DIR / file_name
    with open(temp_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    # replace() is a single, instant step when both folders are on the same drive
    temp_path.replace(INCOMING_DIR / file_name)


def log_file(run_id, file_name, counts, total_rows):
    """Add one line to the generator log (the answer key for testing)."""
    GENERATOR_LOG.parent.mkdir(parents=True, exist_ok=True)
    is_new = not GENERATOR_LOG.exists()
    with open(GENERATOR_LOG, "a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        if is_new:
            writer.writerow(["run_id", "file_name", "written_at", "total_rows",
                             "valid_rows", "messy_rows", "invalid_rows", "duplicate_rows"])
        writer.writerow([run_id, file_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), total_rows,
                         counts["valid"], counts["messy"], counts["invalid"], counts["duplicate"]])


def parse_args():
    parser = argparse.ArgumentParser(description="Generate fake e-commerce events as CSV files.")
    parser.add_argument("--events-per-file", type=int, default=EVENTS_PER_FILE,
                        help=f"rows in each file (default {EVENTS_PER_FILE})")
    parser.add_argument("--interval", type=float, default=FILE_INTERVAL_SECONDS,
                        help=f"seconds between files (default {FILE_INTERVAL_SECONDS})")
    parser.add_argument("--files", type=int, default=0,
                        help="number of files to write, then stop (default 0: run until Ctrl+C)")
    parser.add_argument("--invalid-rate", type=float, default=INVALID_ROW_RATE,
                        help=f"share of invalid rows (default {INVALID_ROW_RATE})")
    parser.add_argument("--duplicate-rate", type=float, default=DUPLICATE_ROW_RATE,
                        help=f"share of duplicate rows (default {DUPLICATE_ROW_RATE})")
    parser.add_argument("--messy-rate", type=float, default=MESSY_ROW_RATE,
                        help=f"share of valid rows with messy formatting (default {MESSY_ROW_RATE})")
    parser.add_argument("--seed", type=int, default=None,
                        help="random seed, to generate the same events every run")
    return parser.parse_args()


def main():
    args = parse_args()
    generator = EventGenerator(seed=args.seed)

    # Each run gets an id, so its files can be told apart in the log.
    # File names include the run id, so a new run never reuses an old name:
    # Spark skips any file name it has already processed.
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    totals = {"valid": 0, "messy": 0, "invalid": 0, "duplicate": 0}
    files_written = 0

    print(f"Writing {args.events_per_file} events every {args.interval}s to {INCOMING_DIR}")
    print("Press Ctrl+C to stop.\n")
    try:
        while args.files == 0 or files_written < args.files:
            rows, counts = generator.make_rows(args.events_per_file, args.invalid_rate,
                                               args.duplicate_rate, args.messy_rate)
            file_name = f"events_{run_id}_{files_written + 1:05d}.csv"
            write_csv_atomically(rows, file_name)
            log_file(run_id, file_name, counts, len(rows))

            files_written += 1
            for kind in totals:
                totals[kind] += counts[kind]
            print(f"[{datetime.now():%H:%M:%S}] {file_name}: {len(rows)} rows "
                  f"({counts['invalid']} invalid, {counts['duplicate']} duplicate, {counts['messy']} messy)")

            if args.files == 0 or files_written < args.files:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped.")

    print(f"\nRun {run_id}: {files_written} files, {sum(totals.values()) - totals['messy']} rows in total.")
    print(f"  Expected in events table:          {totals['valid']} (including {totals['messy']} messy rows)")
    print(f"  Expected in rejected_events table: {totals['invalid']}")
    print(f"  Duplicates (should be skipped):    {totals['duplicate']}")


if __name__ == "__main__":
    main()
