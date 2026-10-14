-- Redshift DDL for the AWSLogEventWarehouse star schema.
--
-- On real AWS this is what `make warehouse` would create instead of DuckDB.
-- Every DISTKEY/SORTKEY/ENCODE choice below is tied to a query pattern from
-- sql/analytics/. DISTSTYLE ALL is used for dimensions because they are small
-- and joined by every fact query; the fact is distributed by its most common
-- join key and sorted by its most common filter columns.

CREATE SCHEMA IF NOT EXISTS warehouse;

-- dim_user: small, joined by every fact query -> replicate to all nodes.
-- Surrogate key is the distribution/sort key for co-located joins.
CREATE TABLE warehouse.dim_user (
    user_sk    BIGINT      NOT NULL ENCODE az64,
    user_id    VARCHAR(64) NOT NULL ENCODE zstd,
    first_seen TIMESTAMP            ENCODE az64,
    last_seen  TIMESTAMP            ENCODE az64,
    is_active  BOOLEAN     NOT NULL ENCODE raw
)
DISTSTYLE ALL
SORTKEY (user_sk);

-- dim_service: tiny lookup used by q1 (hourly error rate) and q3 (route p95).
-- BYTEDICT is ideal for low-cardinality categorical strings.
CREATE TABLE warehouse.dim_service (
    service_sk    BIGINT      NOT NULL ENCODE az64,
    service       VARCHAR(64) NOT NULL ENCODE bytedict,
    service_group VARCHAR(32) NOT NULL ENCODE bytedict
)
DISTSTYLE ALL
SORTKEY (service_sk);

-- dim_page: clickstream funnel attribution (q2).
CREATE TABLE warehouse.dim_page (
    page_sk   BIGINT      NOT NULL ENCODE az64,
    page      VARCHAR(128) NOT NULL ENCODE zstd,
    page_type VARCHAR(32)  NOT NULL ENCODE bytedict
)
DISTSTYLE ALL
SORTKEY (page_sk);

-- dim_device_geo: device x country -> region.
CREATE TABLE warehouse.dim_device_geo (
    device_geo_sk BIGINT      NOT NULL ENCODE az64,
    device        VARCHAR(32) NOT NULL ENCODE bytedict,
    country       VARCHAR(8)  NOT NULL ENCODE bytedict,
    region        VARCHAR(16) NOT NULL ENCODE bytedict
)
DISTSTYLE ALL
SORTKEY (device_geo_sk);

CREATE TABLE warehouse.dim_date (
    date_sk    INTEGER NOT NULL ENCODE az64,
    date       DATE    NOT NULL ENCODE az64,
    year       INTEGER NOT NULL ENCODE az64,
    quarter    INTEGER NOT NULL ENCODE az64,
    month      INTEGER NOT NULL ENCODE az64,
    day        INTEGER NOT NULL ENCODE az64,
    day_of_week INTEGER NOT NULL ENCODE az64,
    day_name   VARCHAR(12) NOT NULL ENCODE bytedict,
    is_weekend BOOLEAN NOT NULL ENCODE raw
)
DISTSTYLE ALL
SORTKEY (date_sk);

-- fact_event_activity: the large fact.
-- * DISTKEY(user_sk): the fact is joined to dim_user (also user_sk) on the
--   funnel/session analyses, so co-locating on user_sk keeps those joins local
--   (no broadcast). dim_service/dim_page are DISTSTYLE ALL so q1/q3 joins are
--   always co-located regardless of distribution.
-- * SORTKEY(dt, service_sk, event_ts): every analytical query filters on dt
--   (30-day window => zone-map pruning of sorted blocks) and q1 further groups
--   by service_sk; event_ts breaks ties for time-ordered scans.
-- * ENCODE: delta on monotonically increasing surrogate keys; zstd on high
--   cardinality text; bytedict on the low-cardinality status/source columns.
CREATE TABLE warehouse.fact_event_activity (
    event_sk           BIGINT       NOT NULL ENCODE delta,
    event_id           VARCHAR(64)  NOT NULL ENCODE zstd,
    event_ts           TIMESTAMP    NOT NULL ENCODE az64,
    dt                 DATE         NOT NULL ENCODE raw,
    hour               INTEGER      NOT NULL ENCODE az64,
    source             VARCHAR(16)  NOT NULL ENCODE bytedict,
    service_sk         BIGINT                ENCODE az64,
    page_sk            BIGINT                ENCODE az64,
    user_sk            BIGINT                ENCODE az64,
    device_geo_sk      BIGINT                ENCODE az64,
    action             VARCHAR(16)           ENCODE bytedict,
    experiment_variant VARCHAR(16)           ENCODE bytedict,
    route              VARCHAR(128)          ENCODE zstd,
    status_code        INTEGER               ENCODE az64,
    status_class       VARCHAR(8)            ENCODE bytedict,
    latency_ms         INTEGER               ENCODE az64,
    dwell_ms           BIGINT                ENCODE az64,
    is_error           BOOLEAN      NOT NULL ENCODE raw,
    ingest_lag_seconds INTEGER               ENCODE az64
)
DISTSTYLE KEY
DISTKEY (user_sk)
SORTKEY (dt, service_sk, event_ts);

-- fact_session_rollup: derived, one row per clickstream session.
CREATE TABLE warehouse.fact_session_rollup (
    session_sk    BIGINT      NOT NULL ENCODE az64,
    session_id    VARCHAR(64) NOT NULL ENCODE zstd,
    user_sk       BIGINT               ENCODE az64,
    device_geo_sk BIGINT               ENCODE az64,
    session_start TIMESTAMP   NOT NULL ENCODE az64,
    session_end   TIMESTAMP   NOT NULL ENCODE az64,
    event_count   INTEGER     NOT NULL ENCODE az64,
    views         INTEGER     NOT NULL ENCODE az64,
    clicks        INTEGER     NOT NULL ENCODE az64,
    add_to_carts  INTEGER     NOT NULL ENCODE az64,
    purchases     INTEGER     NOT NULL ENCODE az64,
    converted     BOOLEAN     NOT NULL ENCODE raw
)
DISTSTYLE KEY
DISTKEY (user_sk)
SORTKEY (session_start);
