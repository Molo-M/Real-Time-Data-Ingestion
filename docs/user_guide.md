# User Guide

Step-by-step instructions for setting up and running the pipeline. The commands are for Windows PowerShell; on macOS or Linux, the same steps apply with the usual differences (`source venv/bin/activate` instead of `venv\Scripts\activate`, and no `winutils` step).

## 1. Requirements

| Requirement | Version used | Notes |
|---|---|---|
| Python | 3.11.9 | **Use 3.11 on Windows.** PySpark's Python workers crash on Windows with Python 3.12 and newer. |
| Java | 17 | Spark runs on Java. Spark 4 also supports Java 21 and 25, but not older versions. Set `JAVA_HOME`. |
| PostgreSQL | 18.6 | Install with the EDB installer from postgresql.org, including the Command Line Tools, and add its `bin` folder to `Path`, so the `psql` command works. |
| Hadoop helpers (Windows only) | 3.3.6 | Spark needs `winutils.exe` and `hadoop.dll` to write files (its checkpoints). Put both in `C:\hadoop\bin`, set `HADOOP_HOME` to `C:\hadoop`, and add `%HADOOP_HOME%\bin` to `Path`. |
| Internet connection | First run only | Spark's PostgreSQL JDBC driver is downloaded into `jars/` the first time Spark starts. |

After changing environment variables, close and reopen every terminal (and VS Code), since programs only see variables that existed when they started.

## 2. Set up the project

**1. Clone the repository and open a terminal in the project folder.**

**2. Create a virtual environment with Python 3.11 and install the packages:**

```powershell
py -3.11 -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Your prompt should now start with `(venv)`. Run `venv\Scripts\activate` again in every new terminal.

**3. Create your `.env` file** from the template. The values already match the database setup script:

```powershell
copy .env.example .env
```

**4. Create the database.** This runs as the `postgres` admin account, so it asks for the password you chose when installing PostgreSQL:

```powershell
psql -U postgres -f sql/postgres_setup.sql
```

It ends with `Setup complete: database ecommerce_streaming with tables events, events_staging and rejected_events.` The two `NOTICE` lines about things that "do not exist, skipping" are expected on the first run. Running the script again later resets the database to empty.

**5. Check that everything works:**

```powershell
python check_setup.py
```

It checks PostgreSQL, Spark, Spark's Python workers, file writing and Spark's connection to PostgreSQL, and should end with five `[PASS]` lines and `All checks passed!`. If a check fails, see [Troubleshooting](#7-troubleshooting).

## 3. Run the pipeline

The pipeline needs two terminals, both with the virtual environment activated.

**Terminal 1: start the stream.**

```powershell
python spark_streaming_to_postgres.py --reset
```

`--reset` starts from scratch: it empties the tables and deletes old data files, Spark's checkpoint and the logs. Leave it out to continue from where the stream last stopped. The stream prints a line for each micro-batch it processes:

```
Streaming started. Watching ...\data\incoming every 5 seconds.
Press Ctrl+C to stop.

Batch 0: 50 rows in 10.0s (5 rows/s)
Batch 1: 100 rows in 3.8s (26 rows/s)
```

**Terminal 2: start the data generator.**

```powershell
python data_generator.py
```

It writes 50 events every 5 seconds, printing a line per file, until you press Ctrl+C. Then it prints what the database should contain:

```
Run 20261106_122702: 5 files, 250 rows in total.
  Expected in events table:          228 (including 8 messy rows)
  Expected in rejected_events table: 15
  Duplicates (should be skipped):    7
```

**To stop:** press Ctrl+C in Terminal 2 (the generator), wait a few seconds for the last batch, then press Ctrl+C in Terminal 1. The stream prints the final row counts. On Windows, a `Terminate batch job (Y/N)?` prompt may flash past; it's answered automatically and is harmless.

### Generator options

| Option | Default | Meaning |
|---|---|---|
| `--events-per-file N` | 50 | Rows in each file |
| `--interval SECONDS` | 5 | Pause between files |
| `--files N` | 0 (no limit) | Stop after N files |
| `--invalid-rate R` | 0.05 | Share of rows that break a rule (should be rejected) |
| `--duplicate-rate R` | 0.02 | Share of rows that repeat a recent event (should be stored once) |
| `--messy-rate R` | 0.05 | Share of valid rows with messy formatting (should be cleaned and kept) |
| `--seed N` | none | Generate the same "random" events on every run |

For example, `python data_generator.py --events-per-file 1000 --interval 1` writes 1,000 events per second.

## 4. Check the results

**Compare the database with the generator's answer key:**

```powershell
python -c "from src.monitor import check_results; check_results()"
```

This compares the tables with `data/metrics/generator_log.csv` and prints `[PASS]` or `[FAIL]` for each check. It only gives a meaningful result if the stream was started with `--reset` before the generator ran, since the log then describes exactly the files in the database.

**Inspect the data with SQL.** The password is `streaming_pass`:

```powershell
psql -U streaming_user -d ecommerce_streaming -f sql/verification_queries.sql
```

This runs 10 queries: row counts, duplicate and cleaning checks, events by type, rejection reasons, the latest events, rows per batch, and latency. Any single query can also be copied into `psql` or pgAdmin.

**Look at the logs** in `data/metrics/`:
- `generator_log.csv`: every file the generator wrote, with its counts of valid, messy, invalid and duplicate rows;
- `batch_log.csv`: every micro-batch, with rows read, inserted and rejected;
- `progress_log.csv`: Spark's own timings for each stage of every batch.

## 5. Use the notebook

`notebooks/Streaming-Pipeline.ipynb` runs the same pipeline step by step: it shows the raw files, each cleaning step, the stream starting, live progress, the database contents, and a final check against the answer key.

1. Open it in VS Code (or Jupyter) and select the virtual environment as the kernel.
2. Open a second terminal with the virtual environment activated.
3. Run the cells **one at a time**, from the top. The notebook says when to run the generator (`python data_generator.py --files 3`, later `python data_generator.py` with no limit) and when to stop it.

**Don't run the notebook's stream and `spark_streaming_to_postgres.py` at the same time.** Both use the same checkpoint (Spark's record of processed files). The notebook's last cells stop its stream.

## 6. Run the tests and performance measurements

Both scripts reset the pipeline and run their own stream, so stop any other stream and generator first.

```powershell
python run_tests.py
```

Runs the 16 test cases in a few minutes and saves the results to `data/metrics/test_results.md`. See [Test Cases](test_cases.md).

```powershell
python performance_test.py
```

Measures throughput and latency at four data rates plus a backlog of 50,000 events, in about 7–10 minutes, and saves the results to `data/metrics/performance_results.md` and `.csv`. Use `--duration 30` for shorter runs. See [Performance Metrics](performance_metrics.md).

## 7. Troubleshooting

| Problem | Cause and fix |
|---|---|
| `Python worker exited unexpectedly (crashed)` | The virtual environment uses Python 3.12 or newer. Check with `python --version`, then recreate it with `py -3.11 -m venv venv`. |
| `UnsatisfiedLinkError: ...NativeIO$Windows.access0` | Spark can't find `hadoop.dll`. Check that both `winutils.exe` and `hadoop.dll` are in `%HADOOP_HOME%\bin`, that this folder is on `Path`, and that `HADOOP_HOME` has no spaces. Restart the terminal. |
| `password authentication failed for user "streaming_user"` | The `.env` file is missing or doesn't match the setup script. Run `copy .env.example .env`. |
| `tables missing` in `check_setup.py` | The setup script hasn't been run, or didn't finish. Run `psql -U postgres -f sql/postgres_setup.sql`. |
| Spark doesn't start (`JAVA_GATEWAY_EXITED`) | Most often, the JDBC driver couldn't be downloaded on the first run. Check the internet connection. Otherwise, check `java -version` (should be 17) and `JAVA_HOME`. |
| `psql` is not recognized | PostgreSQL's `bin` folder (e.g. `C:\Program Files\PostgreSQL\18\bin`) isn't on `Path`. Add it and restart the terminal. |
| New files are never processed | The file names were already processed: Spark skips names it has seen before, even if their content is new. The generator's file names include the time, so this only happens with hand-made files. Use a new name, or restart the stream with `--reset`. |
| `Current batch is falling behind` warning | A micro-batch took longer than the 5-second trigger interval, so the next one starts late. Nothing is lost; events just wait longer. The first batch after starting is always slow. See [Performance Metrics](performance_metrics.md). |
| `Failed to cancel job group ... Cannot find active jobs` | Printed when a stream is stopped between batches. Harmless. |
| Counts don't match the generator's expected numbers | The stream may still be processing the last files: wait a few seconds and check again. Also make sure the stream was started with `--reset` before the generator, so the log and the database describe the same files. |
