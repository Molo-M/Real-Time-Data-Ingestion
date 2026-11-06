| ID | Test | Expected | Actual | Result |
|---|---|---|---|---|
| TC01 | Generated files have the correct columns and row count | 4 files, each with the 10 columns and 100 rows | 4 files, columns correct: True, rows per file: [100] | **PASS** |
| TC02 | Files are written atomically, with unique names | data/tmp/ empty after writing; no repeated file names | 0 file(s) left in data/tmp/, 0 repeated name(s) | **PASS** |
| TC03 | Generator log matches the files written | log rows = rows in files = valid + invalid + duplicate | log 400, files 400, parts 400 | **PASS** |
| TC04 | Files already waiting when the stream starts are processed | 414 rows processed | 414 rows processed | **PASS** |
| TC05 | Files arriving while the stream runs are detected | 200 new rows processed within 2 triggers | 200 new rows processed after 6.0s | **PASS** |
| TC06 | Messy rows are cleaned and kept | ' PURCHASE ' -> 'purchase', '  Desk Lamp ' -> 'Desk Lamp'; 0 badly formatted rows stored | stored as ('purchase', 'Desk Lamp'); 0 badly formatted rows | **PASS** |
| TC07 | Each kind of invalid row is rejected with the right reason | 11 rows: invalid_event_id x1, invalid_event_time x1, invalid_price x1, invalid_quantity x2, invalid_user_id x1, missing_product_name x1, missing_user_id x1, non_positive_price x1, non_positive_quantity x1, unknown_event_type x1 | 11 rows, all reasons match: True | **PASS** |
| TC08 | A duplicate within the same file is stored once | 2 events stored from the controlled file (the duplicate skipped) | 2 events stored | **PASS** |
| TC09 | total_amount is calculated correctly | every total_amount = price x quantity (e.g. 89.99 x 2 = 179.98) | 0 wrong totals; example: 179.98 | **PASS** |
| TC10 | Every valid event is stored exactly once | 570 events (all unique) | 570 events, 570 unique | **PASS** |
| TC11 | Every invalid row is stored in rejected_events | 33 rejected rows | 33 rejected rows | **PASS** |
| TC12 | No stream errors, and the staging table is emptied after each batch | no exception; 0 rows in events_staging | exception: None; 0 rows in events_staging | **PASS** |
| TC13 | After a restart, only new files are processed | 100 new rows processed, nothing reprocessed | 100 new rows processed; events match answer key: True | **PASS** |
| TC14 | A retried batch doesn't duplicate anything | same counts after re-running batch 2: 664 events, 38 rejected | 664 events, 38 rejected | **PASS** |
| TC15 | Batches finish within the trigger interval (after the first) | every batch under 5s (trigger: 5 seconds) | 2 batches, average 5.0s, slowest 5.4s | **FAIL** |
| TC16 | 95% of events reach PostgreSQL within the latency limit | 95th percentile latency under 15s (events written while the stream runs) | 95th percentile 5.06s (median 5.05s, max 5.06s) | **PASS** |
