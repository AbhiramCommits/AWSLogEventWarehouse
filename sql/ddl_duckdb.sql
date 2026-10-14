-- DuckDB warehouse DDL for AWSLogEventWarehouse.
--
-- DuckDB stands in locally for Redshift/Athena. It reads the curated Parquet
-- directly from S3 (via httpfs) as an external scan, then materialises the
-- star schema below into artifacts/warehouse.duckdb for analytical queries.
--
-- Surrogate keys are deterministic 64-bit hashes of the natural key so they
-- are stable across reloads and cheap to compute without a key lookup.

CREATE SCHEMA IF NOT EXISTS warehouse;
USE warehouse;

-- ---------------------------------------------------------------------------
-- Dimensions
-- ---------------------------------------------------------------------------

-- dim_date: one row per calendar day present in the data.
CREATE TABLE IF NOT EXISTS dim_date (
    date_sk      INTEGER PRIMARY KEY,   -- yyyymmdd
    date         DATE    NOT NULL,
    year         INTEGER NOT NULL,
    quarter      INTEGER NOT NULL,
    month        INTEGER NOT NULL,
    day          INTEGER NOT NULL,
    day_of_week  INTEGER NOT NULL,       -- 1=Monday .. 7=Sunday
    day_name     VARCHAR NOT NULL,
    is_weekend   BOOLEAN NOT NULL
);

-- dim_service: application service (app_log). DISTSTYLE ALL on Redshift.
CREATE TABLE IF NOT EXISTS dim_service (
    service_sk     BIGINT PRIMARY KEY,
    service        VARCHAR NOT NULL,
    service_group  VARCHAR NOT NULL
);

-- dim_page: clickstream page, with a coarse page type for funnel analysis.
CREATE TABLE IF NOT EXISTS dim_page (
    page_sk    BIGINT PRIMARY KEY,
    page       VARCHAR NOT NULL,
    page_type  VARCHAR NOT NULL
);

-- dim_user: SCD Type 1 (latest attributes win). See docs/data_model.md for the
-- Type 2 extension (valid_from/valid_to/is_current + a versioned surrogate).
CREATE TABLE IF NOT EXISTS dim_user (
    user_sk    BIGINT PRIMARY KEY,
    user_id    VARCHAR NOT NULL,
    first_seen TIMESTAMP,
    last_seen  TIMESTAMP,
    is_active  BOOLEAN NOT NULL DEFAULT TRUE
);

-- dim_device_geo: degenerate device + country -> region lookup.
CREATE TABLE IF NOT EXISTS dim_device_geo (
    device_geo_sk BIGINT PRIMARY KEY,
    device        VARCHAR NOT NULL,
    country       VARCHAR NOT NULL,
    region        VARCHAR NOT NULL
);

-- ---------------------------------------------------------------------------
-- Facts
-- ---------------------------------------------------------------------------

-- fact_event_activity: grain = one curated event (app_log OR clickstream).
CREATE TABLE IF NOT EXISTS fact_event_activity (
    event_sk            BIGINT PRIMARY KEY,
    event_id            VARCHAR NOT NULL,
    event_ts            TIMESTAMP NOT NULL,
    dt                  DATE NOT NULL,
    hour                INTEGER NOT NULL,
    source              VARCHAR NOT NULL,
    service_sk          BIGINT,          -- app_log only
    page_sk             BIGINT,          -- clickstream only
    user_sk             BIGINT,
    device_geo_sk       BIGINT,          -- clickstream only
    action              VARCHAR,         -- clickstream only
    experiment_variant  VARCHAR,         -- clickstream only
    route               VARCHAR,         -- app_log only
    status_code         INTEGER,         -- app_log only
    status_class        VARCHAR,         -- app_log only
    latency_ms          INTEGER,         -- app_log only
    dwell_ms            BIGINT,          -- clickstream only
    is_error            BOOLEAN NOT NULL,
    ingest_lag_seconds  INTEGER
);

-- fact_session_rollup: optional derived fact, grain = one clickstream session.
CREATE TABLE IF NOT EXISTS fact_session_rollup (
    session_sk      BIGINT PRIMARY KEY,
    session_id      VARCHAR NOT NULL,
    user_sk         BIGINT,
    device_geo_sk   BIGINT,
    session_start   TIMESTAMP NOT NULL,
    session_end     TIMESTAMP NOT NULL,
    event_count     INTEGER NOT NULL,
    views           INTEGER NOT NULL,
    clicks          INTEGER NOT NULL,
    add_to_carts    INTEGER NOT NULL,
    purchases       INTEGER NOT NULL,
    converted       BOOLEAN NOT NULL
);
