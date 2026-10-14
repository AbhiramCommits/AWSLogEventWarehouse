-- Athena DDL for the AWSLogEventWarehouse curated tables.
--
-- Athena queries the curated Parquet in place (no load step). Partition
-- projection is used instead of MSCK REPAIR / crawler-registered partitions so
-- queries prune by dt without a catalog round-trip; the same tables are what
-- the QuickSight dataset in dashboard/quicksight/ points at.
--
-- Replace <CURATED_BUCKET> with the curated bucket name (default
-- `logwarehouse-curated`).

CREATE DATABASE IF NOT EXISTS logwarehouse;

-- Curated application logs (one row per de-duplicated app_log event).
CREATE EXTERNAL TABLE IF NOT EXISTS logwarehouse.curated_app_log (
    event_id            string,
    event_time          string,
    service             string,
    host                string,
    level               string,
    status_code         int,
    latency_ms          int,
    route               string,
    user_id             string,
    trace_id            string,
    message             string,
    source              string,
    ingest_time         string,
    event_ts            timestamp,
    hour                string,
    ingest_lag_seconds  bigint,
    is_null_key         boolean,
    status_class        string,
    is_error            boolean
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://<CURATED_BUCKET>/curated/source=app_log/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'projection.enabled'='true',
    'projection.dt.type'='date',
    'projection.dt.range'='2020-01-01,NOW',
    'projection.dt.format'='yyyy-MM-dd',
    'projection.dt.interval'='1',
    'projection.dt.interval.unit'='DAYS',
    'storage.location.template'='s3://<CURATED_BUCKET>/curated/source=app_log/dt=${dt}'
);

-- Curated clickstream events.
CREATE EXTERNAL TABLE IF NOT EXISTS logwarehouse.curated_clickstream (
    event_id            string,
    event_time          string,
    session_id          string,
    user_id             string,
    page                string,
    referrer            string,
    device              string,
    country             string,
    action              string,
    dwell_ms            int,
    experiment_variant  string,
    source              string,
    ingest_time         string,
    event_ts            timestamp,
    hour                string,
    ingest_lag_seconds  bigint,
    is_null_key         boolean,
    is_error            boolean
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://<CURATED_BUCKET>/curated/source=clickstream/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'projection.enabled'='true',
    'projection.dt.type'='date',
    'projection.dt.range'='2020-01-01,NOW',
    'projection.dt.format'='yyyy-MM-dd',
    'projection.dt.interval'='1',
    'projection.dt.interval.unit'='DAYS',
    'storage.location.template'='s3://<CURATED_BUCKET>/curated/source=clickstream/dt=${dt}'
);

-- Quarantined (unparseable) raw rows, kept for reprocessing.
CREATE EXTERNAL TABLE IF NOT EXISTS logwarehouse.quarantine (
    raw_payload     string,
    reason          string,
    source          string,
    quarantined_at  timestamp
)
STORED AS PARQUET
LOCATION 's3://<CURATED_BUCKET>/quarantine/';

-- Example: the q1 hourly error-rate query can now run as-is against Athena by
-- replacing the DuckDB fact/dim joins with scans of curated_app_log.
-- SELECT date_trunc('hour', event_ts) AS h, service,
--        avg(CASE WHEN is_error THEN 1.0 ELSE 0 END) AS error_rate
-- FROM logwarehouse.curated_app_log
-- WHERE dt >= '2024-01-01'
-- GROUP BY 1, 2;
