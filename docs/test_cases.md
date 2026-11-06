# Test Cases

This test plan checks every item in the project brief's "What to Test" list, plus the pipeline's ability to recover from restarts and retries. Each test case lists the steps to run it by hand, the expected outcome and the actual outcome. All 16 test cases are also automated in `run_tests.py`, which produced the actual results below.

## Test environment

| Item | Value |
|---|---|
| Date of the recorded run | 6 November 2026 |
| Operating system | Windows |
| Python | 3.11.9 |
| Java | 17.0.12 |
| Apache Spark (PySpark) | 4.3.0, local mode using all CPU cores |
| PostgreSQL | 18.6 |
| PostgreSQL JDBC driver | 42.7.4 |
| Trigger interval | 5 seconds, at most 20 files per micro-batch |

## How to run the tests

**Automatically:** stop any running stream and generator, then run `python run_tests.py`. It resets the pipeline, runs all 16 test cases in a few minutes, prints `[PASS]` or `[FAIL]` for each, and saves the results to `data/metrics/test_results.md`.

**By hand:** follow the steps for each test case below. Start from an empty pipeline with `python spark_streaming_to_postgres.py --reset` (stopped again with Ctrl+C where a step needs the stream stopped). The SQL queries can be run in `psql -U streaming_user -d ecommerce_streaming` (password `streaming_pass`), and `sql/verification_queries.sql` contains ready-made versions of most of them.

**The answer key.** The generator records in `data/metrics/generator_log.csv` exactly how many valid, messy, invalid and duplicate rows it wrote to each file, so every test of the database compares it against known correct numbers. Test cases TC06 to TC08 use a hand-made file, `events_controlled_test.csv`, in which the correct outcome of every row is known in advance: 2 valid rows (one of them messy), 1 duplicate, and 11 rows that each break exactly one rule.

## Coverage

| Question from the project brief | Test cases |
|---|---|
| Are the CSV files being generated correctly? | TC01, TC02, TC03 |
| Is Spark detecting and processing new files? | TC04, TC05 |
| Are the data transformations correct? | TC06, TC07, TC08, TC09 |
| Is data being written into PostgreSQL without errors? | TC10, TC11, TC12 |
| *(added)* Does the pipeline recover from restarts and retries? | TC13, TC14 |
| Are performance metrics within expected limits? | TC15, TC16 |

## Results summary

**15 of 16 test cases passed.** Every functional test passed: the files were generated correctly, Spark detected every file, every transformation was correct, and every event was stored exactly once, including after a restart and a retried batch. The one failure, TC15, is a performance finding: micro-batches took slightly longer than the 5-second trigger interval. It's explained [below](#analysis-of-tc15).

## Test cases

### Data generation

| ID | Test | Steps | Expected | Actual | Result |
|---|---|---|---|---|---|
| TC01 | Generated files have the correct columns and row count | Run `python data_generator.py --files 4 --events-per-file 100`. Open each file in `data/incoming/`. | 4 files, each with the 10 columns in order and 100 rows | 4 files, columns correct, 100 rows in every file | **PASS** |
| TC02 | Files are written atomically, with unique names | After TC01, check `data/tmp/` and the file names. | `data/tmp/` empty (every file moved after being written); no repeated file names | 0 files left in `data/tmp/`; 0 repeated names | **PASS** |
| TC03 | The generator log matches the files written | Add up the columns of `data/metrics/generator_log.csv` and compare them with the files. | Rows in the log = rows in the files = valid + invalid + duplicate | Log 400, files 400, valid + invalid + duplicate 400 | **PASS** |

### Detecting and processing new files

| ID | Test | Steps | Expected | Actual | Result |
|---|---|---|---|---|---|
| TC04 | Files already waiting when the stream starts are processed | Write files (as in TC01) *before* starting the stream. Start it, then add up `rows_read` in `data/metrics/batch_log.csv`. | Every row processed (414 rows: 4 files of 100 plus the 14-row controlled file) | 414 rows processed | **PASS** |
| TC05 | Files arriving while the stream runs are detected | With the stream running, run `python data_generator.py --files 2 --events-per-file 100`. Watch the stream's output. | 200 new rows processed within 2 triggers (10 seconds) | 200 new rows processed after 6.0 seconds | **PASS** |

### Transformations

| ID | Test | Steps | Expected | Actual | Result |
|---|---|---|---|---|---|
| TC06 | Messy rows are cleaned and kept | Process the controlled file. Query the messy row: `SELECT event_type, product_name FROM events WHERE event_id = '22222222-2222-4222-8222-222222222222';` and check query 3 of `verification_queries.sql`. | `' PURCHASE '` stored as `purchase`, `'  Desk Lamp '` as `Desk Lamp`; 0 badly formatted rows in `events` | Stored as (`purchase`, `Desk Lamp`); 0 badly formatted rows | **PASS** |
| TC07 | Each kind of invalid row is rejected with the right reason | Query `SELECT reject_reason, COUNT(*) FROM rejected_events WHERE source_file = 'events_controlled_test.csv' GROUP BY 1;` | 11 rows: `invalid_event_id`, `invalid_event_time`, `invalid_price`, `invalid_user_id`, `missing_product_name`, `missing_user_id`, `non_positive_price`, `non_positive_quantity`, `unknown_event_type` (1 each) and `invalid_quantity` (2: `"two"` and `"1.5"`) | 11 rows; every reason matches | **PASS** |
| TC08 | A duplicate within the same file is stored once | Query `SELECT COUNT(*) FROM events WHERE source_file = 'events_controlled_test.csv';` | 2 events (the duplicate of the first valid row skipped) | 2 events | **PASS** |
| TC09 | `total_amount` is calculated correctly | Run query 4 of `verification_queries.sql`, and check the messy row (price 89.99, quantity 2). | 0 rows where `total_amount` ≠ price × quantity; the messy row's total is 179.98 | 0 wrong totals; 179.98 | **PASS** |

### Writing to PostgreSQL

| ID | Test | Steps | Expected | Actual | Result |
|---|---|---|---|---|---|
| TC10 | Every valid event is stored exactly once | Compare query 2 of `verification_queries.sql` with the total of `valid_rows` in the generator log. | 570 events, all with unique `event_id`s | 570 events, 570 unique | **PASS** |
| TC11 | Every invalid row is stored in `rejected_events` | Compare the row count of `rejected_events` with the total of `invalid_rows` in the generator log. | 33 rejected rows | 33 rejected rows | **PASS** |
| TC12 | No stream errors, and the staging table is emptied after every batch | Check that the stream is still running with no error, and run query 1 of `verification_queries.sql`. | No exception; 0 rows in `events_staging` | No exception; 0 rows in `events_staging` | **PASS** |

### Recovery

| ID | Test | Steps | Expected | Actual | Result |
|---|---|---|---|---|---|
| TC13 | After a restart, only new files are processed | Stop the stream (Ctrl+C). Write 1 file of 100 rows. Restart the stream **without** `--reset`. Check the new batch in `batch_log.csv` and the row counts. | Only the 100 new rows processed (nothing reprocessed); `events` still matches the answer key | 100 new rows processed; `events` matches the answer key | **PASS** |
| TC14 | A retried batch doesn't duplicate anything | Run the last finished batch again through `write_batch()` with its own batch number, as Spark does after a failure (automated in `run_tests.py`). Compare the row counts before and after. | Same counts before and after: 664 events, 38 rejected | 664 events, 38 rejected | **PASS** |

### Performance

| ID | Test | Steps | Expected | Actual | Result |
|---|---|---|---|---|---|
| TC15 | Micro-batches finish within the trigger interval | Check `trigger_ms` in `data/metrics/progress_log.csv` for every batch after the first (the first includes one-off start-up work). | Every batch under 5 seconds | 2 batches: average 5.0 seconds, slowest 5.4 seconds | **FAIL** |
| TC16 | 95% of events reach PostgreSQL within 15 seconds | Run query 9 of `verification_queries.sql` for events written while the stream was running. | 95th percentile latency under 15 seconds | 95th percentile 5.06 seconds (median 5.05, maximum 5.06) | **PASS** |

## Analysis of TC15

TC15 checks whether each micro-batch finishes before the next one is due. On the test machine, batches took about 5.0 to 5.4 seconds against the 5-second trigger interval, so Spark reported `Current batch is falling behind`.

**What it means in practice.** When a batch overruns, Spark doesn't skip or lose anything: it starts the next batch as soon as the previous one finishes, and that batch takes all the files that have arrived meanwhile. Events wait slightly longer, but every event is still processed exactly once, as TC10 to TC14 confirm. TC16 shows that latency stayed well within its limit.

**The cause.** The performance measurements show that each micro-batch has a fixed cost of about 4.5 seconds on this Windows machine, whatever its size: a 50-row batch took 4.66 seconds on average, and a 37,500-row batch only 7.82 seconds. Part of this cost is Spark's checkpoint bookkeeping, which goes through the slower Hadoop file layer on Windows, and part is `write_batch()` running four separate Spark jobs to count rows and opening two database connections. See [Performance Metrics](performance_metrics.md) for the details and possible improvements.

**Status.** Recorded as a known limitation rather than fixed, since it affects latency only, not correctness. Reducing the per-batch cost, or lengthening the trigger interval, would make TC15 pass.

## Additional manual tests

These were run by hand on the test machine while the pipeline was being built.

| ID | Test | Steps | Expected | Actual | Result |
|---|---|---|---|---|---|
| M1 | The environment is set up correctly | Run `python check_setup.py`. | 5 checks pass: PostgreSQL, Spark, Python workers, file writing, Spark to PostgreSQL | All 5 checks passed (after fixing defect D1) | **PASS** |
| M2 | The stream stops cleanly with Ctrl+C on Windows | Run the stream and the generator. Stop the generator, then press Ctrl+C in the stream's terminal. | The stream stops and prints the final row counts, with no error traceback | Printed `Rows in PostgreSQL: 232 events, 15 rejected` with no traceback (after fixing defect D2) | **PASS** |

## Defects found and fixed

| ID | Found by | Problem | Cause | Fix |
|---|---|---|---|---|
| D1 | M1 | The Python workers check failed: `Python worker exited unexpectedly (crashed)`. | The JDBC driver was loaded with `spark.jars.packages`, which also sends the downloaded jar to the Python workers; on Windows, this crashed them. | The driver is now downloaded into `jars/` by the project and loaded with `spark.jars`, which gives it to Spark's Java side only. |
| D2 | M2 | Pressing Ctrl+C crashed the stream with `Cannot call methods on a stopped SparkContext`, before it printed the final counts. | On Windows, Ctrl+C reaches every program in the terminal, so Spark's Java process shut down before Python tried to stop the stream. | The shutdown now tries each step separately and treats "Spark has already stopped" as normal, and the final counts are read with psycopg, which doesn't need Spark. |
