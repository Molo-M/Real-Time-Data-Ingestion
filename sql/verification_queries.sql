-- ======================================================================
-- verification_queries.sql
-- Queries for checking the pipeline's results by hand.
--
-- Run them all from the project folder (password: streaming_pass):
--     psql -U streaming_user -d ecommerce_streaming -f sql/verification_queries.sql
-- or copy any single query into psql or pgAdmin.
-- ======================================================================

\echo '\n1. How many rows each table holds (events_staging should always be 0)'
SELECT 'events' AS table_name, COUNT(*) AS row_count FROM events
UNION ALL SELECT 'rejected_events', COUNT(*) FROM rejected_events
UNION ALL SELECT 'events_staging', COUNT(*) FROM events_staging;

\echo '\n2. No event is stored twice (the two counts should be equal)'
SELECT COUNT(*) AS stored_rows, COUNT(DISTINCT event_id) AS unique_events FROM events;

\echo '\n3. Messy rows were cleaned (both counts should be 0)'
SELECT
    COUNT(*) FILTER (WHERE event_type <> lower(trim(event_type))) AS badly_formatted_types,
    COUNT(*) FILTER (WHERE product_name <> trim(product_name))    AS names_with_extra_spaces
FROM events;

\echo '\n4. total_amount is always price x quantity (should be 0)'
SELECT COUNT(*) AS wrong_totals FROM events WHERE total_amount <> price * quantity;

\echo '\n5. Events by type'
SELECT event_type, COUNT(*) AS events, SUM(total_amount) AS total_value
FROM events GROUP BY event_type ORDER BY events DESC;

\echo '\n6. Why rows were rejected, with an example of each'
SELECT reject_reason, COUNT(*) AS rejected_rows,
       MIN(event_type) AS example_event_type, MIN(price) AS example_price,
       MIN(quantity) AS example_quantity, MIN(event_time) AS example_event_time
FROM rejected_events GROUP BY reject_reason ORDER BY rejected_rows DESC;

\echo '\n7. The 10 most recently stored events'
SELECT event_time, user_id, event_type, product_name, price, quantity, total_amount, batch_id
FROM events ORDER BY inserted_at DESC, event_time DESC LIMIT 10;

\echo '\n8. Rows written per micro-batch'
SELECT batch_id, COUNT(*) AS events, MIN(ingested_at) AS processed_at
FROM events GROUP BY batch_id ORDER BY batch_id;

\echo '\n9. End-to-end latency in seconds (event happening -> stored in PostgreSQL)'
SELECT ROUND(AVG(EXTRACT(EPOCH FROM inserted_at - event_time))::numeric, 2) AS avg_seconds,
       ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM inserted_at - event_time)))::numeric, 2) AS median_seconds,
       ROUND((PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM inserted_at - event_time)))::numeric, 2) AS p95_seconds,
       ROUND(MAX(EXTRACT(EPOCH FROM inserted_at - event_time))::numeric, 2) AS max_seconds
FROM events;

\echo '\n10. Where the time goes: waiting + reading (event -> Spark) vs writing (Spark -> table)'
SELECT ROUND(AVG(EXTRACT(EPOCH FROM ingested_at - event_time))::numeric, 2) AS avg_until_spark_seconds,
       ROUND(AVG(EXTRACT(EPOCH FROM inserted_at - ingested_at))::numeric, 2) AS avg_writing_seconds
FROM events;
