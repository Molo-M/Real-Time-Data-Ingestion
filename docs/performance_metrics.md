# Performance Metrics

## Summary

| Metric | Result |
|---|---|
| Correctness under load | 100%: every scenario, up to 300,000 events, matched the generator's answer key exactly |
| Maximum processing speed | About **7,000 events per second** (a backlog of 50,000 events processed in one 7.1-second micro-batch) |
| Highest sustained rate | **5,000 events per second**, at the pipeline's limit (it processed 4,796 per second while working) |
| End-to-end latency | **5.4 seconds** on average at 10 events per second; **10.1 seconds** at 5,000 events per second |
| 95th percentile latency | Under 15 seconds at every rate (13.75 seconds at the highest) |
| Micro-batch duration | 4.6 to 7.8 seconds on average, often just over the 5-second trigger interval |

The main finding: **every micro-batch has a fixed cost of about 4.5 seconds on the test machine, whatever its size.** That cost, rather than the amount of data, sets both the pipeline's minimum latency and its maximum throughput.

## Test environment

| Item | Value |
|---|---|
| Date | 6 November 2026 |
| Machine | Windows laptop; Spark in local mode using all CPU cores |
| Software | Python 3.11.9, Java 17.0.12, Spark 4.3.0, PostgreSQL 18.6, JDBC driver 42.7.4 |
| Pipeline settings | Trigger interval 5 seconds; at most 20 files per micro-batch; 4 shuffle partitions |
| Data | 5% invalid rows, 2% duplicates, 5% messy rows (the generator's defaults) |

## How it was measured

`performance_test.py` ran five scenarios, each starting from an empty pipeline:

| Scenario | Data written |
|---|---|
| Low | 50 events every 5 seconds (10 events/s) for 60 seconds |
| Medium | 200 events every 2 seconds (100 events/s) for 60 seconds |
| High | 1,000 events every second (1,000 events/s) for 60 seconds |
| Very high | 5,000 events every second (5,000 events/s) for 60 seconds |
| Backlog | 50,000 events (20 files of 2,500) already waiting when the stream starts |

In each rate scenario, the stream ran while files were written at a fixed pace. After the last file, the test waited until every row had been processed, then collected the measurements. A short warm-up ran first, so Spark's one-off start-up cost (loading the JDBC driver and opening the first connections) wasn't counted in the first scenario.

**What the metrics mean:**
- **Batch time:** Spark's own measurement of each micro-batch, from start to finish (`triggerExecution`), recorded by a streaming query listener (`src/metrics.py`).
- **Processing rate:** rows processed divided by the time spent processing batches. This is the pipeline's speed *while working*. At low data rates it simply equals the input rate, since each batch only has a few rows to process.
- **Time in `write_batch()`:** the share of each batch spent in `write_batch()` (Spark's `addBatch` stage), which cleans the batch, counts it and writes it to PostgreSQL. The rest is Spark's own work: finding new files, planning, and recording progress in the checkpoint.
- **Latency:** from the three timestamps stored with every event: `event_time` (the event happened), `ingested_at` (Spark processed its batch) and `inserted_at` (it reached the `events` table).

## Results

### Throughput and micro-batch processing

| Scenario | Events generated | Batches | Avg rows per batch | Avg batch time (s) | Slowest batch (s) | Batches over 5 s | Processing rate (events/s) | Time in `write_batch()` | Caught up after the last file (s) | Results correct |
|---|---|---|---|---|---|---|---|---|---|---|
| Low (10 events/s) | 600 | 12 | 50 | 4.66 | 5.66 | 2 | 11 | 54% | 5.0 | Yes |
| Medium (100 events/s) | 6,000 | 13 | 462 | 4.59 | 4.98 | 0 | 100 | 54% | 8.1 | Yes |
| High (1,000 events/s) | 60,000 | 12 | 5,000 | 5.52 | 6.61 | 11 | 906 | 62% | 8.1 | Yes |
| Very high (5,000 events/s) | 300,000 | 8 | 37,500 | 7.82 | 8.20 | 8 | 4,796 | 74% | 8.1 | Yes |
| Backlog (50,000 at once) | 50,000 | 1 | 50,000 | 7.14 | 7.14 | 1 | 6,999 | 76% | n/a | Yes |

### End-to-end latency, in seconds

| Scenario | Average | Median | 95th percentile | Maximum | Avg until Spark processed it | Avg writing to PostgreSQL |
|---|---|---|---|---|---|---|
| Low (10 events/s) | 5.37 | 5.25 | 6.31 | 6.31 | 4.31 | 1.06 |
| Medium (100 events/s) | 6.84 | 6.93 | 9.03 | 9.19 | 5.77 | 1.07 |
| High (1,000 events/s) | 7.64 | 7.75 | 10.31 | 11.18 | 5.93 | 1.71 |
| Very high (5,000 events/s) | 10.08 | 9.99 | 13.75 | 14.19 | 7.66 | 2.42 |
| Backlog (50,000 at once) | n/a | n/a | n/a | n/a | n/a | n/a |

Latency isn't meaningful for the backlog scenario, since its events were waiting before the stream started.

## Analysis

### 1. The results stayed correct under load

In every scenario, including 300,000 events at 5,000 per second, the `events` and `rejected_events` tables matched the generator's answer key exactly. Batches that overran the trigger interval, and the much larger batches that resulted, didn't cause a single lost or duplicated event.

### 2. Each micro-batch has a large fixed cost

Batch time barely depends on batch size:

| Rows per batch | Avg batch time |
|---|---|
| 50 | 4.66 s |
| 462 | 4.59 s |
| 5,000 | 5.52 s |
| 37,500 | 7.82 s |

A batch 750 times bigger takes less than twice as long. Roughly, each batch costs **about 4.5 seconds regardless of size, plus less than 0.1 seconds per 1,000 rows**. So most of a small batch's time isn't spent processing data at all.

**Where the fixed cost comes from.** Spark's timings for each stage of a batch show two sources. In the backlog batch, for example (7,144 ms in total):

| Stage | Time | What it does |
|---|---|---|
| `addBatch` (`write_batch()`) | 5,449 ms | Cleaning, counting and writing the 50,000 rows |
| `latestOffset` | 557 ms | Listing `data/incoming/` to find new files |
| `walCommit` | 557 ms | Recording in the checkpoint which files the batch will read |
| `commitOffsets` | 556 ms | Recording in the checkpoint that the batch finished |
| Planning and preparing | 23 ms | |

- **Checkpoint bookkeeping takes about 1.7 seconds per batch.** The three file-system stages each took about 0.55 seconds, whatever the batch size. On Windows, Spark's file operations go through the Hadoop compatibility layer (`winutils`), which is likely why these simple steps are this slow.
- **`write_batch()` has fixed costs of its own.** Even in the low scenario, where batches had only 50 rows, `write_batch()` took 54% of 4.66 seconds, about 2.5 seconds. For each batch, it runs four separate Spark jobs just to count rows (rows read, valid, unique and rejected), opens two database connections, makes two JDBC writes and runs the merge. Each of these costs time regardless of how many rows there are.

### 3. Throughput: about 5,000 to 7,000 events per second

Because the fixed cost is paid once per batch, larger batches are more efficient. The processing rate rose from 11 events per second (low) to 4,796 (very high) and 6,999 (backlog), where the fixed cost was spread over 50,000 rows.

At 5,000 events per second, the pipeline processed 4,796 per second while working: **right at its limit**. It finished each file only slightly slower than files arrived, and caught up 8 seconds after the last one. At a higher sustained rate, it would fall further and further behind.

### 4. When batches overrun the trigger, the stream balances itself

The trigger interval schedules a new micro-batch every 5 seconds. When a batch takes longer, Spark starts the next one as soon as the previous one finishes, and that batch takes every file that arrived in the meantime. This is why batches at 5,000 events per second held 37,500 rows on average: about 7.5 seconds of data each, matching their 7.8-second duration.

This is self-balancing: falling behind makes batches bigger, and bigger batches are more efficient. It's why the pipeline kept up with 5,000 events per second even though every batch overran the trigger.

### 5. Latency: mostly waiting, not writing

At low rates, most of the latency is time spent before Spark processes an event (4.31 of 5.37 seconds). An event waits for the next trigger (2.5 seconds on average, with a 5-second trigger), then for the batch's file listing, checkpoint bookkeeping and cleaning. Writing to PostgreSQL took about 1 second.

As the rate rises, both parts grow: batches take longer, and when a batch overruns, the next one starts late, so events wait longer. Writing time also grows with batch size (2.42 seconds at the highest rate). Even so, 95% of events reached PostgreSQL within 15 seconds at every rate.

### 6. The first batch after starting is slow

The first batch after the stream starts took 10 to 21 seconds in different runs, because it includes one-off work: loading the JDBC driver, opening the first database connections, and Spark compiling its query code. Later batches don't repeat this work. The performance test runs a warm-up batch first so that this cost doesn't distort the measurements, and test case TC15 excludes the first batch for the same reason.

## Were the metrics within expected limits?

The project brief asks whether performance is "within expected limits". These limits were set for a near-real-time pipeline, where events should be stored within seconds, not minutes:

| Limit | Target | Result | Met? |
|---|---|---|---|
| Correctness | Every event stored exactly once, every invalid row rejected | Matched the answer key in every scenario | Yes |
| Latency | 95% of events stored within 15 seconds | 6.3 to 13.8 seconds | Yes |
| Keeping up | Process at least 1,000 events per second, and catch up within 10 seconds of the data stopping | Kept up with up to 5,000 events per second; caught up within 8.1 seconds | Yes |
| Batch duration | Each micro-batch finishes within the 5-second trigger interval | 4.6 to 7.8 seconds on average; over 5 seconds in most batches at 1,000 events per second and above | No |

The batch-duration limit was not met (also test case TC15). Its effect is limited to latency, which stayed within its own limit, but it shows that the pipeline is close to its capacity on this machine.

## Possible improvements

In order of their likely effect on the fixed cost per batch:

1. **Count each batch in one Spark job instead of four.** `write_batch()` counts rows read, valid, unique and rejected separately; a single aggregation can produce all four numbers at once.
2. **Use one database connection per batch.** `write_batch()` opens two psycopg connections; sharing one saves a connection setup per batch.
3. **Lengthen the trigger interval at high data rates.** A 10-second trigger would halve the number of batches, and so the total fixed cost, at the price of higher latency. The trigger interval is the main trade-off between latency and efficiency.
4. **Run on Linux, or on a cluster.** The checkpoint bookkeeping is likely slowed by Spark's Windows file layer. On a cluster, Spark would also spread each batch's work across several machines.

## How to reproduce

Stop any running stream and generator, then run:

```powershell
python performance_test.py
```

It takes about 7 to 10 minutes and saves the results to `data/metrics/performance_results.md` and `data/metrics/performance_results.csv`. Use `--duration 30` for shorter runs. Results vary between machines and between runs on the same machine.
