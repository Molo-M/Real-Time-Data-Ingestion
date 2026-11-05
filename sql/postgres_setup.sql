-- ======================================================================
-- postgres_setup.sql
-- Creates the database, user and tables for the e-commerce streaming project.
--
-- Run it once as the postgres superuser, from the project folder:
--     psql -U postgres -f sql/postgres_setup.sql
--
-- Running it again resets everything: the database is dropped and recreated,
-- which gives you a clean, empty database for testing.
--
-- The password below is for a LOCAL PRACTICE database only. In a real
-- project it would never be written in a committed file.
-- ======================================================================

-- Start from a clean slate
DROP DATABASE IF EXISTS ecommerce_streaming WITH (FORCE);
DROP ROLE IF EXISTS streaming_user;

-- A dedicated user for the pipeline, instead of the postgres superuser
CREATE ROLE streaming_user WITH LOGIN PASSWORD 'streaming_pass';

-- The database, owned by that user
CREATE DATABASE ecommerce_streaming OWNER streaming_user;

-- Switch to the new database, as the new user, so the tables belong to it
\connect ecommerce_streaming
SET ROLE streaming_user;

-- ----------------------------------------------------------------------
-- events: every clean, valid event from the stream
-- ----------------------------------------------------------------------
CREATE TABLE events (
    event_id      UUID          PRIMARY KEY,            -- one row per event, no duplicates
    event_time    TIMESTAMP     NOT NULL,               -- when the user did it (from the generator)
    user_id       INTEGER       NOT NULL,
    session_id    VARCHAR(36)   NOT NULL,
    event_type    VARCHAR(20)   NOT NULL
                  CHECK (event_type IN ('view', 'add_to_cart', 'purchase')),
    product_id    INTEGER       NOT NULL,
    product_name  VARCHAR(200)  NOT NULL,
    category      VARCHAR(50)   NOT NULL,
    price         NUMERIC(10,2) NOT NULL CHECK (price > 0),
    quantity      INTEGER       NOT NULL CHECK (quantity > 0),
    total_amount  NUMERIC(12,2) NOT NULL,               -- price x quantity
    source_file   VARCHAR(255),                         -- which CSV the event came from
    batch_id      BIGINT        NOT NULL,               -- which Spark micro-batch wrote it
    ingested_at   TIMESTAMP     NOT NULL,               -- when Spark processed it
    inserted_at   TIMESTAMP     NOT NULL DEFAULT now()  -- when it arrived in this table
);

-- Speed up the queries used for checking the data and measuring performance
CREATE INDEX idx_events_event_time ON events (event_time);
CREATE INDEX idx_events_event_type ON events (event_type);

-- ----------------------------------------------------------------------
-- events_staging: each micro-batch is written here first, then copied
-- into events while skipping event_ids that are already there. If Spark
-- retries a batch after a failure, no event is inserted twice.
-- ----------------------------------------------------------------------
CREATE TABLE events_staging (LIKE events INCLUDING DEFAULTS);

-- ----------------------------------------------------------------------
-- rejected_events: rows that failed validation, kept for inspection.
-- Values are stored as text, exactly as they appeared in the CSV.
-- ----------------------------------------------------------------------
CREATE TABLE rejected_events (
    reject_id      BIGSERIAL    PRIMARY KEY,
    event_id       TEXT,
    event_time     TEXT,
    user_id        TEXT,
    session_id     TEXT,
    event_type     TEXT,
    product_id     TEXT,
    product_name   TEXT,
    category       TEXT,
    price          TEXT,
    quantity       TEXT,
    reject_reason  TEXT         NOT NULL,               -- why the row was rejected
    source_file    VARCHAR(255),
    batch_id       BIGINT       NOT NULL,
    ingested_at    TIMESTAMP    NOT NULL,
    inserted_at    TIMESTAMP    NOT NULL DEFAULT now()
);

RESET ROLE;

\echo 'Setup complete: database ecommerce_streaming with tables events, events_staging and rejected_events.'
