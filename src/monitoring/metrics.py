"""Operational metrics computed from the warehouse and published to CloudWatch.

Metrics published under the ``LogWarehouse/Pipeline`` namespace:

* ``PipelineFreshnessMinutes`` — minutes since the newest event.
* ``IngestLagP50/P95/P99`` — ingest lag percentiles (seconds).
* ``ErrorRate`` — overall and per-service error rate (percent).
* ``QuarantineRate`` / ``DuplicateRate`` — pipeline data-quality rates (percent).
* ``EventsLastHour`` / ``EventsLastHourDropPct`` / ``TotalEvents`` — volume.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from typing import Any

import duckdb

from logwarehouse.aws import client
from logwarehouse.config import Settings, get_settings

NAMESPACE = "LogWarehouse/Pipeline"


def _connect(settings: Settings) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(settings.duckdb_path), read_only=True)
    con.execute("SET TimeZone='UTC';")
    con.execute("USE warehouse;")
    return con


def _scalar(con: duckdb.DuckDBPyConnection, query: str) -> Any:
    row = con.execute(query).fetchone()
    return row[0] if row else None


def compute_metrics(settings: Settings | None = None) -> dict[str, Any]:
    """Compute the operational metric snapshot from the warehouse + reports."""
    settings = settings or get_settings()
    con = _connect(settings)
    try:
        freshness = _scalar(
            con, "SELECT date_diff('second', max(event_ts), now()) / 60.0 FROM fact_event_activity"
        )
        total_events = _scalar(con, "SELECT count(*) FROM fact_event_activity")
        lags = con.execute(
            "SELECT quantile_cont(ingest_lag_seconds, [0.5, 0.95, 0.99]) "
            "FROM fact_event_activity WHERE ingest_lag_seconds IS NOT NULL"
        ).fetchone()[0]
        error_overall = _scalar(
            con,
            "SELECT avg(CASE WHEN is_error THEN 1.0 ELSE 0.0 END) FROM fact_event_activity "
            "WHERE source = 'app_log'",
        )
        by_service = con.execute(
            "SELECT s.service, avg(CASE WHEN f.is_error THEN 1.0 ELSE 0.0 END) AS rate "
            "FROM fact_event_activity f JOIN dim_service s USING (service_sk) "
            "WHERE f.source = 'app_log' GROUP BY s.service ORDER BY rate DESC"
        ).fetchall()
        hourly = con.execute(
            "SELECT date_trunc('hour', event_ts) AS h, count(*) AS c "
            "FROM fact_event_activity GROUP BY h ORDER BY h"
        ).fetchall()
        by_source = con.execute(
            "SELECT source, count(*) FROM fact_event_activity "
            "WHERE event_ts >= (SELECT max(event_ts) - INTERVAL 1 HOUR FROM fact_event_activity) "
            "GROUP BY source"
        ).fetchall()
    finally:
        con.close()

    # Use the most recent *complete* hour: the latest bucket is usually a
    # partial clock-hour and would otherwise look like a volume collapse.
    if len(hourly) >= 2:
        complete = hourly[:-1]
        events_last_hour = complete[-1][1]
        latest_hour = complete[-1][0]
    elif hourly:
        complete = hourly
        events_last_hour = hourly[-1][1]
        latest_hour = hourly[-1][0]
    else:
        complete, events_last_hour, latest_hour = [], 0, None
    # Compare like-for-like against the same hour-of-day on prior days to avoid
    # false positives from the diurnal traffic curve.
    same_hour = [c for h, c in complete[:-1] if latest_hour is not None and h.hour == latest_hour.hour]
    trailing = same_hour if same_hour else [c for _, c in complete[-7:-1]]
    trailing_avg = sum(trailing) / len(trailing) if trailing else float(events_last_hour)
    drop_pct = ((trailing_avg - events_last_hour) / trailing_avg * 100) if trailing_avg else 0.0

    transform = _read_report(settings, "transform_report.json")
    totals = transform.get("totals", {})
    rows_in = totals.get("rows_in") or 1
    quarantine_rate = totals.get("quarantined_rows", 0) / rows_in * 100
    duplicate_rate = totals.get("duplicates_removed", 0) / rows_in * 100

    result = {
        "generated_at": datetime.now(UTC).isoformat(),
        "freshness_minutes": round(float(freshness or 0), 2),
        "total_events": int(total_events or 0),
        "events_last_hour": int(events_last_hour),
        "events_last_hour_by_source": dict(by_source),
        "trailing_hourly_avg": round(trailing_avg, 2),
        "events_last_hour_drop_pct": round(drop_pct, 2),
        "ingest_lag_p50": round(float(lags[0]), 2) if lags else 0.0,
        "ingest_lag_p95": round(float(lags[1]), 2) if lags else 0.0,
        "ingest_lag_p99": round(float(lags[2]), 2) if lags else 0.0,
        "error_rate_overall": round(float(error_overall or 0) * 100, 4),
        "error_rate_by_service": {s: round(r * 100, 4) for s, r in by_service},
        "quarantine_rate": round(quarantine_rate, 4),
        "duplicate_rate": round(duplicate_rate, 4),
        "incident_active": False,
    }
    return _apply_incident_override(result, settings)


_INCIDENT_FIELDS = (
    "freshness_minutes",
    "events_last_hour",
    "events_last_hour_by_source",
    "events_last_hour_drop_pct",
    "error_rate_overall",
    "error_rate_by_service",
    "quarantine_rate",
    "duplicate_rate",
)


def _apply_incident_override(metrics: dict[str, Any], settings: Settings) -> dict[str, Any]:
    """Overlay a simulated incident window if ``incident_active.json`` exists."""
    path = settings.artifacts_dir / "incident_active.json"
    if not path.exists():
        return metrics
    payload = json.loads(path.read_text())
    degraded = payload.get("degraded", {})
    for field in _INCIDENT_FIELDS:
        if field in degraded:
            metrics[field] = degraded[field]
    metrics["incident_active"] = True
    metrics["scenario"] = degraded.get("scenario", payload.get("scenario"))
    return metrics

def _read_report(settings: Settings, name: str) -> dict[str, Any]:
    path = settings.artifacts_dir / name
    return json.loads(path.read_text()) if path.exists() else {}


def publish_metrics(metrics: dict[str, Any], settings: Settings | None = None) -> int:
    """Publish the metric snapshot to CloudWatch. Returns the datapoint count."""
    settings = settings or get_settings()
    cloudwatch = client("cloudwatch", settings)
    now = datetime.now(UTC)
    data: list[dict[str, Any]] = [
        {"MetricName": "PipelineFreshnessMinutes", "Value": metrics["freshness_minutes"],
         "Unit": "Minutes", "Timestamp": now},
        {"MetricName": "IngestLagP50", "Value": metrics["ingest_lag_p50"],
         "Unit": "Seconds", "Timestamp": now},
        {"MetricName": "IngestLagP95", "Value": metrics["ingest_lag_p95"],
         "Unit": "Seconds", "Timestamp": now},
        {"MetricName": "IngestLagP99", "Value": metrics["ingest_lag_p99"],
         "Unit": "Seconds", "Timestamp": now},
        {"MetricName": "ErrorRate", "Value": metrics["error_rate_overall"],
         "Unit": "Percent", "Timestamp": now},
        {"MetricName": "QuarantineRate", "Value": metrics["quarantine_rate"],
         "Unit": "Percent", "Timestamp": now},
        {"MetricName": "DuplicateRate", "Value": metrics["duplicate_rate"],
         "Unit": "Percent", "Timestamp": now},
        {"MetricName": "EventsLastHour", "Value": metrics["events_last_hour"],
         "Unit": "Count", "Timestamp": now},
        {"MetricName": "EventsLastHourDropPct", "Value": metrics["events_last_hour_drop_pct"],
         "Unit": "Percent", "Timestamp": now},
        {"MetricName": "TotalEvents", "Value": metrics["total_events"],
         "Unit": "Count", "Timestamp": now},
    ]
    for service, rate in metrics["error_rate_by_service"].items():
        data.append(
            {"MetricName": "ErrorRate", "Value": rate, "Unit": "Percent", "Timestamp": now,
             "Dimensions": [{"Name": "Service", "Value": service}]}
        )
    for source, count in metrics["events_last_hour_by_source"].items():
        data.append(
            {"MetricName": "EventsLastHour", "Value": count, "Unit": "Count", "Timestamp": now,
             "Dimensions": [{"Name": "Source", "Value": source}]}
        )
    for i in range(0, len(data), 20):
        cloudwatch.put_metric_data(Namespace=NAMESPACE, MetricData=data[i : i + 20])
    return len(data)


def run() -> dict[str, Any]:
    """Compute + publish metrics and write ``artifacts/metrics_snapshot.json``."""
    settings = get_settings()
    metrics = compute_metrics(settings)
    published = publish_metrics(metrics, settings)
    metrics["published_datapoints"] = published
    settings.artifacts_dir.mkdir(parents=True, exist_ok=True)
    (settings.artifacts_dir / "metrics_snapshot.json").write_text(json.dumps(metrics, indent=2))
    return metrics


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    argparse.ArgumentParser(description="Compute and publish pipeline metrics").parse_args(argv)
    started = time.perf_counter()
    metrics = run()
    print(json.dumps(metrics, indent=2))
    print(f"published {metrics['published_datapoints']} datapoints in "
          f"{time.perf_counter() - started:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
