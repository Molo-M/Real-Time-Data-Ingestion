"""
performance_test.py
Measures the pipeline's performance at increasing data rates.

Each scenario starts from an empty pipeline, runs the generator at a fixed rate
while the stream runs, waits until every event is stored, then measures:
  - throughput: how many events per second Spark processes while working
  - batch duration: how long each micro-batch takes (it should stay under the
    5-second trigger interval, or the stream falls behind)
  - latency: how long events take from happening to being stored in PostgreSQL

A final "backlog" scenario starts the stream with 50,000 events already waiting,
to measure the maximum processing speed.

Results are printed and saved to data/metrics/performance_results.csv and
data/metrics/performance_results.md, ready to copy into docs/performance_metrics.md.

WARNING: this resets the pipeline (empties the tables and deletes the data files).

Usage:
    python performance_test.py                  # 60 seconds per scenario, about 7 minutes in total
    python performance_test.py --duration 30    # shorter runs
"""

import argparse
import csv
import time
from datetime import datetime

from data_generator import EventGenerator, log_file, write_csv_atomically
from src import monitor
from src.config import BATCH_LOG, METRICS_DIR, PROGRESS_LOG, TRIGGER_INTERVAL
from src.metrics import latency_summary
from src.postgres_sink import table_counts
from src.spark_session import get_spark
from src.streaming import reset_pipeline, start_stream

# (name, events per file, seconds between files)
SCENARIOS = [
    ("Low (10 events/s)", 50, 5.0),
    ("Medium (100 events/s)", 200, 2.0),
    ("High (1,000 events/s)", 1000, 1.0),
    ("Very high (5,000 events/s)", 5000, 1.0),
]
BACKLOG_FILES, BACKLOG_ROWS_PER_FILE = 20, 2500   # 50,000 events waiting at once

TRIGGER_SECONDS = float(TRIGGER_INTERVAL.split()[0])
RESULTS_CSV = METRICS_DIR / "performance_results.csv"
RESULTS_MD = METRICS_DIR / "performance_results.md"


def write_files(generator, run_id, count, rows_per_file, interval=0.0):
    """Write files like data_generator.py does, at a fixed pace."""
    next_time = time.time()
    for number in range(1, count + 1):
        rows, counts = generator.make_rows(rows_per_file, 0.05, 0.02, 0.05)
        name = f"events_{run_id}_{number:05d}.csv"
        write_csv_atomically(rows, name)
        log_file(run_id, name, counts, len(rows))
        if number < count:   # no pause after the last file
            next_time += interval
            time.sleep(max(0.0, next_time - time.time()))


def measure(name, target_rate, wall_seconds, catch_up_seconds, include_latency=True):
    """Collect the results of one scenario from the logs and the database."""
    progress = monitor.read_log(PROGRESS_LOG)
    batches = monitor.read_log(BATCH_LOG)
    rows = sum(int(p["num_input_rows"]) for p in progress)
    batch_seconds = [int(p["trigger_ms"]) / 1000 for p in progress]
    # Share of batch time spent in write_batch() (cleaning, counting and writing);
    # the rest is Spark finding new files, planning and updating its checkpoint
    write_share = (sum(int(p["add_batch_ms"]) for p in progress) / max(sum(int(p["trigger_ms"]) for p in progress), 1))
    latency = latency_summary() if include_latency else {}
    counts = table_counts()
    expected = monitor.expected_totals()
    return {
        "scenario": name,
        "target_events_per_s": target_rate,
        "events_generated": expected["rows"],
        "events_stored": counts["events"],
        "rejected_stored": counts["rejected_events"],
        "results_correct": counts["events"] == expected["valid"] and counts["rejected_events"] == expected["invalid"],
        "batches": len(progress),
        "avg_rows_per_batch": round(rows / max(len(progress), 1)),
        "avg_batch_s": round(sum(batch_seconds) / max(len(batch_seconds), 1), 2),
        "max_batch_s": round(max(batch_seconds, default=0), 2),
        "batches_over_trigger": sum(1 for s in batch_seconds if s > TRIGGER_SECONDS),
        # Events per second while Spark is working: total rows / time spent in batches
        "processing_rate_per_s": round(rows / max(sum(batch_seconds), 0.001)),
        "write_share_pct": round(100 * write_share),
        "catch_up_s": round(catch_up_seconds, 1),
        "wall_s": round(wall_seconds, 1),
        "latency_avg_s": latency.get("avg_s"),
        "latency_median_s": latency.get("median_s"),
        "latency_p95_s": latency.get("p95_s"),
        "latency_max_s": latency.get("max_s"),
        "latency_until_spark_s": latency.get("avg_until_spark_s"),
        "latency_writing_s": latency.get("avg_writing_s"),
        "batches_logged": len(batches),
    }


def run_rate_scenario(spark, name, rows_per_file, interval, duration):
    """Generate at a fixed rate while the stream runs, then measure."""
    reset_pipeline()
    query = start_stream(spark)
    time.sleep(2)   # let the stream initialize before data arrives
    files = max(1, round(duration / interval))
    print(f"\n{name}: {files} files of {rows_per_file} events, one every {interval:g}s ...")
    started = time.time()
    write_files(EventGenerator(seed=42), datetime.now().strftime("%H%M%S"), files, rows_per_file, interval)
    generator_done = time.time()
    monitor.wait_until_caught_up(timeout=600, quiet=True)
    caught_up = time.time()
    time.sleep(1)   # let the listener record the final batch
    query.stop()
    return measure(name, round(rows_per_file / interval), caught_up - started, caught_up - generator_done)


def run_backlog_scenario(spark):
    """Start the stream with 50,000 events already waiting: the maximum speed."""
    reset_pipeline()
    total = BACKLOG_FILES * BACKLOG_ROWS_PER_FILE
    print(f"\nBacklog: {total:,} events waiting before the stream starts ...")
    write_files(EventGenerator(seed=42), "backlog", BACKLOG_FILES, BACKLOG_ROWS_PER_FILE)
    started = time.time()
    query = start_stream(spark)
    monitor.wait_until_caught_up(timeout=900, quiet=True)
    finished = time.time()
    time.sleep(1)
    query.stop()
    # Latency isn't meaningful here: the events were waiting before the stream started
    return measure(f"Backlog ({total:,} events at once)", "-", finished - started, 0, include_latency=False)


def warm_up(spark):
    """Run a small batch first, so the one-off start-up cost (loading the JDBC
    driver, first connections) isn't counted in the first scenario."""
    print("Warming up ...")
    reset_pipeline()
    query = start_stream(spark)
    write_files(EventGenerator(seed=1), "warmup", 1, 50)
    monitor.wait_until_caught_up(timeout=120, quiet=True)
    query.stop()


def save(results):
    RESULTS_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULTS_CSV, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)

    def value(v):
        return "n/a" if v is None else v

    lines = [
        "### Throughput and batch processing", "",
        "| Scenario | Events generated | Batches | Avg rows/batch | Avg batch time (s) | Slowest batch (s) "
        "| Batches over trigger | Processing rate (events/s) | Time in write_batch() | Caught up after generator stopped (s) | Results correct |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        lines.append(f"| {r['scenario']} | {r['events_generated']:,} | {r['batches']} | {r['avg_rows_per_batch']:,} "
                     f"| {r['avg_batch_s']} | {r['max_batch_s']} | {r['batches_over_trigger']} "
                     f"| {r['processing_rate_per_s']:,} | {r['write_share_pct']}% | {r['catch_up_s']} "
                     f"| {'Yes' if r['results_correct'] else 'No'} |")
    lines += [
        "", "### End-to-end latency (event happening -> stored in PostgreSQL), in seconds", "",
        "| Scenario | Average | Median | 95th percentile | Maximum | Avg until Spark processed it | Avg writing to PostgreSQL |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in results:
        lines.append(f"| {r['scenario']} | {value(r['latency_avg_s'])} | {value(r['latency_median_s'])} "
                     f"| {value(r['latency_p95_s'])} | {value(r['latency_max_s'])} "
                     f"| {value(r['latency_until_spark_s'])} | {value(r['latency_writing_s'])} |")
    RESULTS_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Measure the pipeline's performance at increasing rates.")
    parser.add_argument("--duration", type=float, default=60, help="seconds of data per scenario (default 60)")
    args = parser.parse_args()

    print("This resets the pipeline before each scenario (tables, data files, checkpoint and logs).")
    spark = get_spark()
    results = []
    try:
        warm_up(spark)
        for name, rows_per_file, interval in SCENARIOS:
            results.append(run_rate_scenario(spark, name, rows_per_file, interval, args.duration))
            r = results[-1]
            print(f"  {r['batches']} batches, avg {r['avg_batch_s']}s, processing {r['processing_rate_per_s']:,} events/s, "
                  f"p95 latency {r['latency_p95_s']}s, results correct: {r['results_correct']}")
        results.append(run_backlog_scenario(spark))
        r = results[-1]
        print(f"  {r['events_generated']:,} events in {r['wall_s']}s, processing {r['processing_rate_per_s']:,} events/s, "
              f"results correct: {r['results_correct']}")
    finally:
        spark.stop()

    save(results)
    print(f"\nResults saved to {RESULTS_CSV.name} and {RESULTS_MD.name} in {METRICS_DIR}\n")
    print(RESULTS_MD.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()