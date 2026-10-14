"""Load the dimensional model from curated Parquet into DuckDB.

DuckDB reads the curated Parquet straight from the S3 endpoint (``httpfs``),
materialises the star schema defined in ``sql/ddl_duckdb.sql`` and validates
referential integrity. The database is written to ``artifacts/warehouse.duckdb``.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

import duckdb

from logwarehouse.config import Settings, get_settings

SK = "9223372036854775807"

_DDL_PATH = Path(__file__).resolve().parents[2] / "sql" / "ddl_duckdb.sql"


def connect(settings: Settings | None = None) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection with the S3/httpfs configuration applied."""
    settings = settings or get_settings()
    settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(settings.duckdb_path))
    con.execute("INSTALL httpfs; LOAD httpfs;")
    endpoint = (settings.endpoint_url or "http://localhost:4566").replace("http://", "").replace(
        "https://", ""
    )
    use_ssl = "false" if settings.endpoint_url.startswith("http://") else "true"
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute(f"SET s3_use_ssl={use_ssl};")
    con.execute("SET s3_url_style='path';")
    con.execute(f"SET s3_access_key_id='{settings.access_key}';")
    con.execute(f"SET s3_secret_access_key='{settings.secret_key}';")
    con.execute(f"SET s3_region='{settings.region}';")
    con.execute("SET TimeZone='UTC';")
    return con


def _parquet_glob(settings: Settings, source: str) -> str:
    return settings.s3_uri(
        settings.curated_bucket, settings.curated_prefix, f"source={source}", "dt=*", "*.parquet"
    )


def _views(settings: Settings) -> list[str]:
    """Create staging views over the curated Parquet for both sources."""
    statements = []
    for source in ("app_log", "clickstream"):
        glob = _parquet_glob(settings, source)
        statements.append(
            f"CREATE OR REPLACE VIEW stage_{source} AS "
            f"SELECT *, CAST(dt AS DATE) AS _dt, CAST(hour AS INTEGER) AS _hour "
            f"FROM read_parquet('{glob}', hive_partitioning=1);"
        )
    return statements


def _sk(expr: str) -> str:
    """Return a SQL expression for a positive 64-bit surrogate key."""
    return f"CAST(hash({expr}) % {SK} AS BIGINT)"


def load_dimensions(settings: Settings) -> list[str]:
    """Populate every dimension table from the staging views."""
    return [
        # dim_date
        """
        INSERT INTO dim_date
        SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_sk, d AS date,
               year(d) AS year, quarter(d) AS quarter, month(d) AS month, day(d) AS day,
               dayofweek(d) AS day_of_week, dayname(d) AS day_name,
               (dayofweek(d) IN (0, 6)) AS is_weekend
        FROM (SELECT DISTINCT _dt AS d FROM stage_app_log
              UNION SELECT DISTINCT _dt FROM stage_clickstream) t;
        """,
        # dim_service
        f"""
        INSERT INTO dim_service
        SELECT {_sk('service')} AS service_sk, service,
               CASE service
                   WHEN 'auth' THEN 'identity'
                   WHEN 'search' THEN 'discovery'
                   WHEN 'catalog' THEN 'discovery'
                   WHEN 'checkout' THEN 'commerce'
                   WHEN 'payments' THEN 'commerce'
                   WHEN 'recommendations' THEN 'discovery'
                   ELSE 'other' END AS service_group
        FROM (SELECT DISTINCT service FROM stage_app_log WHERE service IS NOT NULL) s;
        """,
        # dim_page
        f"""
        INSERT INTO dim_page
        SELECT {_sk('page')} AS page_sk, page,
               CASE
                   WHEN page = '/home' THEN 'home'
                   WHEN page LIKE '/search%' THEN 'search'
                   WHEN page LIKE '/product%' THEN 'product'
                   WHEN page = '/cart' THEN 'cart'
                   WHEN page = '/checkout' THEN 'checkout'
                   WHEN page = '/account' THEN 'account'
                   ELSE 'other' END AS page_type
        FROM (SELECT DISTINCT page FROM stage_clickstream WHERE page IS NOT NULL) p;
        """,
        # dim_device_geo
        f"""
        INSERT INTO dim_device_geo
        SELECT {_sk("device || '|' || country")} AS device_geo_sk, device, country,
               CASE
                   WHEN country IN ('US','CA','MX') THEN 'Americas'
                   WHEN country IN ('GB','DE','FR') THEN 'EMEA'
                   WHEN country IN ('IN','JP','AU') THEN 'APAC'
                   WHEN country = 'BR' THEN 'LATAM'
                   ELSE 'other' END AS region
        FROM (SELECT DISTINCT device, country FROM stage_clickstream
              WHERE device IS NOT NULL AND country IS NOT NULL) g;
        """,
        # dim_user (SCD Type 1: latest wins, first/last seen from events)
        f"""
        INSERT INTO dim_user
        SELECT {_sk('user_id')} AS user_sk, user_id,
               min(event_ts) AS first_seen, max(event_ts) AS last_seen, TRUE AS is_active
        FROM (SELECT user_id, event_ts FROM stage_app_log WHERE user_id IS NOT NULL
              UNION ALL
              SELECT user_id, event_ts FROM stage_clickstream WHERE user_id IS NOT NULL) u
        GROUP BY user_id;
        """,
    ]


def load_facts(settings: Settings) -> list[str]:
    """Populate the fact tables."""
    return [
        f"""
        INSERT INTO fact_event_activity
        SELECT {_sk("source || '|' || event_id")} AS event_sk, event_id, event_ts, _dt AS dt,
               _hour AS hour, source,
               {_sk('service')} AS service_sk, NULL AS page_sk, {_sk('user_id')} AS user_sk,
               NULL AS device_geo_sk, NULL AS action, NULL AS experiment_variant,
               route, status_code, status_class, latency_ms, NULL AS dwell_ms,
               is_error, CAST(ingest_lag_seconds AS INTEGER) AS ingest_lag_seconds
        FROM stage_app_log
        UNION ALL
        SELECT {_sk("source || '|' || event_id")}, event_id, event_ts, _dt, _hour, source,
               NULL, {_sk('page')}, {_sk('user_id')}, {_sk("device || '|' || country")},
               action, experiment_variant, NULL, NULL, NULL, NULL, dwell_ms, is_error,
               CAST(ingest_lag_seconds AS INTEGER)
        FROM stage_clickstream;
        """,
        f"""
        INSERT INTO fact_session_rollup
        SELECT {_sk('session_id')} AS session_sk, session_id,
               {_sk('user_id')} AS user_sk,
               {_sk("device || '|' || country")} AS device_geo_sk,
               min(event_ts) AS session_start, max(event_ts) AS session_end,
               count(*) AS event_count,
               sum(CASE WHEN action = 'view' THEN 1 ELSE 0 END) AS views,
               sum(CASE WHEN action = 'click' THEN 1 ELSE 0 END) AS clicks,
               sum(CASE WHEN action = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_carts,
               sum(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS purchases,
               (sum(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) > 0) AS converted
        FROM stage_clickstream
        WHERE session_id IS NOT NULL
        GROUP BY session_id, user_id, device, country;
        """,
    ]


def _scalar(con: duckdb.DuckDBPyConnection, query: str) -> Any:
    return con.execute(query).fetchone()[0]


def validate(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Run referential-integrity and key-uniqueness checks."""
    curated = _scalar(
        con,
        "SELECT (SELECT count(*) FROM stage_app_log) + (SELECT count(*) FROM stage_clickstream)",
    )
    fact = _scalar(con, "SELECT count(*) FROM fact_event_activity")
    checks = {
        "curated_rows": curated,
        "fact_rows": fact,
        "fact_matches_curated": curated == fact,
        "orphan_service_keys": _scalar(
            con,
            "SELECT count(*) FROM fact_event_activity f LEFT JOIN dim_service d "
            "USING (service_sk) WHERE f.service_sk IS NOT NULL AND d.service_sk IS NULL",
        ),
        "orphan_page_keys": _scalar(
            con,
            "SELECT count(*) FROM fact_event_activity f LEFT JOIN dim_page d "
            "USING (page_sk) WHERE f.page_sk IS NOT NULL AND d.page_sk IS NULL",
        ),
        "orphan_user_keys": _scalar(
            con,
            "SELECT count(*) FROM fact_event_activity f LEFT JOIN dim_user d "
            "USING (user_sk) WHERE f.user_sk IS NOT NULL AND d.user_sk IS NULL",
        ),
        "orphan_device_geo_keys": _scalar(
            con,
            "SELECT count(*) FROM fact_event_activity f LEFT JOIN dim_device_geo d "
            "USING (device_geo_sk) WHERE f.device_geo_sk IS NOT NULL AND d.device_geo_sk IS NULL",
        ),
        "duplicate_event_sk": _scalar(
            con,
            "SELECT count(*) - count(DISTINCT event_sk) FROM fact_event_activity",
        ),
        "dim_user_rows": _scalar(con, "SELECT count(*) FROM dim_user"),
        "dim_user_duplicate_sk": _scalar(
            con, "SELECT count(*) - count(DISTINCT user_sk) FROM dim_user"
        ),
        "session_rollup_rows": _scalar(con, "SELECT count(*) FROM fact_session_rollup"),
    }
    checks["passed"] = all(
        [
            checks["fact_matches_curated"],
            checks["orphan_service_keys"] == 0,
            checks["orphan_page_keys"] == 0,
            checks["orphan_user_keys"] == 0,
            checks["orphan_device_geo_keys"] == 0,
            checks["duplicate_event_sk"] == 0,
            checks["dim_user_duplicate_sk"] == 0,
        ]
    )
    return checks


def run(rebuild: bool = True) -> dict[str, Any]:
    """Build the DuckDB warehouse and write ``artifacts/warehouse_report.json``."""
    settings = get_settings()
    started = time.perf_counter()
    con = connect(settings)
    try:
        if rebuild:
            con.execute("DROP SCHEMA IF EXISTS warehouse CASCADE;")
        con.execute(_DDL_PATH.read_text())
        for stmt in _views(settings):
            con.execute(stmt)
        for stmt in load_dimensions(settings):
            con.execute(stmt)
        for stmt in load_facts(settings):
            con.execute(stmt)
        checks = validate(con)
    finally:
        con.close()
    checks["wall_seconds"] = round(time.perf_counter() - started, 3)
    settings.artifacts_dir.mkdir(parents=True, exist_ok=True)
    (settings.artifacts_dir / "warehouse_report.json").write_text(json.dumps(checks, indent=2))
    return checks


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Load the DuckDB warehouse")
    parser.parse_args(argv)
    checks = run()
    print(json.dumps(checks, indent=2))
    return 0 if checks["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
