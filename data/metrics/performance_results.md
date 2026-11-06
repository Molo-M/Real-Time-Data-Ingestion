### Throughput and batch processing

| Scenario | Events generated | Batches | Avg rows/batch | Avg batch time (s) | Slowest batch (s) | Batches over trigger | Processing rate (events/s) | Time writing to PostgreSQL | Caught up after generator stopped (s) | Results correct |
|---|---|---|---|---|---|---|---|---|---|---|
| Low (10 events/s) | 600 | 12 | 50 | 4.66 | 5.66 | 2 | 11 | 54% | 5.0 | Yes |
| Medium (100 events/s) | 6,000 | 13 | 462 | 4.59 | 4.98 | 0 | 100 | 54% | 8.1 | Yes |
| High (1,000 events/s) | 60,000 | 12 | 5,000 | 5.52 | 6.61 | 11 | 906 | 62% | 8.1 | Yes |
| Very high (5,000 events/s) | 300,000 | 8 | 37,500 | 7.82 | 8.2 | 8 | 4,796 | 74% | 8.1 | Yes |
| Backlog (50,000 events at once) | 50,000 | 1 | 50,000 | 7.14 | 7.14 | 1 | 6,999 | 76% | 0 | Yes |

### End-to-end latency (event happening -> stored in PostgreSQL), in seconds

| Scenario | Average | Median | 95th percentile | Maximum | Avg until Spark processed it | Avg writing to PostgreSQL |
|---|---|---|---|---|---|---|
| Low (10 events/s) | 5.37 | 5.25 | 6.31 | 6.31 | 4.31 | 1.06 |
| Medium (100 events/s) | 6.84 | 6.93 | 9.03 | 9.19 | 5.77 | 1.07 |
| High (1,000 events/s) | 7.64 | 7.75 | 10.31 | 11.18 | 5.93 | 1.71 |
| Very high (5,000 events/s) | 10.08 | 9.99 | 13.75 | 14.19 | 7.66 | 2.42 |
| Backlog (50,000 events at once) | n/a | n/a | n/a | n/a | n/a | n/a |
