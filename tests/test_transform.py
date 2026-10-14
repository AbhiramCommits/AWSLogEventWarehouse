"""Tests for the PySpark Glue transform job.

A single local Spark session is shared across the module (session scope) to
keep the suite fast.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from glue.transform_job import run_transform, spark_session

BASE = datetime(2024, 6, 1, 10, 0, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = spark_session("transform-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _app_event(
    event_id: str,
    *,
    offset_minutes: int = 0,
    ingest_offset_seconds: int = 30,
    level: str = "INFO",
    status: int = 200,
    user_id: str | None = "u1",
    service: str = "auth",
) -> dict:
    event_time = BASE + timedelta(minutes=offset_minutes)
    return {
        "source": "app_log",
        "event_id": event_id,
        "event_time": event_time.isoformat(),
        "service": service,
        "host": "h1",
        "level": level,
        "status_code": status,
        "latency_ms": 12,
        "route": "/api/v1/login",
        "user_id": user_id,
        "trace_id": "t" + event_id,
        "message": "msg",
        "ingest_time": (event_time + timedelta(seconds=ingest_offset_seconds)).isoformat(),
    }


def _write_records(path: Path, records: list[dict | str]) -> None:
    path.mkdir(parents=True, exist_ok=True)
    lines = [r if isinstance(r, str) else json.dumps(r) for r in records]
    (path / "part-0.json").write_text("\n".join(lines) + "\n")


def _run(spark: SparkSession, tmp_path: Path, records: list[dict | str]) -> dict:
    raw = tmp_path / "raw"
    _write_records(raw, records)
    return run_transform(spark, str(raw), str(tmp_path / "curated"),
                         str(tmp_path / "quarantine"), "app_log")


def test_row_and_defect_counts(spark: SparkSession, tmp_path: Path) -> None:
    records = [
        _app_event("e1"),
        _app_event("e1", ingest_offset_seconds=300),  # duplicate, later ingest
        _app_event("e2", service="search"),
        "{\"source\": \"app_log\", BROKEN",
        _app_event("e3", user_id=None),
    ]
    report = _run(spark, tmp_path, records)
    assert report["rows_in"] == 5
    assert report["duplicates_removed"] == 1
    assert report["quarantined_rows"] == 1
    assert report["null_key_rows"] == 1
    assert report["rows_out"] == 2


def test_dedupe_keeps_earliest_ingest(spark: SparkSession, tmp_path: Path) -> None:
    records = [
        _app_event("e1", ingest_offset_seconds=600),
        _app_event("e1", ingest_offset_seconds=10),
    ]
    raw = tmp_path / "raw"
    _write_records(raw, records)
    run_transform(spark, str(raw), str(tmp_path / "curated"),
                  str(tmp_path / "quarantine"), "app_log")
    curated = spark.read.parquet(str(tmp_path / "curated"))
    rows = curated.collect()
    assert len(rows) == 1
    # the earliest ingest_time maps to the smallest lag
    assert rows[0]["ingest_lag_seconds"] == 10


def test_quarantined_rows_are_isolated(spark: SparkSession, tmp_path: Path) -> None:
    records = [_app_event("e1"), "not json at all", "{broken"]
    report = _run(spark, tmp_path, records)
    assert report["quarantined_rows"] == 2
    curated = spark.read.parquet(str(tmp_path / "curated"))
    assert curated.count() == 1
    quarantine = spark.read.json(str(tmp_path / "quarantine"))
    assert quarantine.count() == 2
    reasons = {r["reason"] for r in quarantine.collect()}
    assert reasons == {"json_parse_error"}


def test_partition_and_lag_derivation(spark: SparkSession, tmp_path: Path) -> None:
    records = [_app_event("e1", offset_minutes=0, ingest_offset_seconds=45)]
    raw = tmp_path / "raw"
    _write_records(raw, records)
    run_transform(spark, str(raw), str(tmp_path / "curated"),
                  str(tmp_path / "quarantine"), "app_log")
    row = spark.read.parquet(str(tmp_path / "curated")).collect()[0]
    assert str(row["dt"]) == "2024-06-01"
    assert row["hour"] == "10"
    assert row["ingest_lag_seconds"] == 45


def test_enrichment_columns(spark: SparkSession, tmp_path: Path) -> None:
    records = [
        _app_event("e1", status=500, level="ERROR"),
        _app_event("e2", status=404, level="WARN"),
    ]
    raw = tmp_path / "raw"
    _write_records(raw, records)
    run_transform(spark, str(raw), str(tmp_path / "curated"),
                  str(tmp_path / "quarantine"), "app_log")
    df = spark.read.parquet(str(tmp_path / "curated"))
    by_id = {r["event_id"]: r for r in df.collect()}
    assert by_id["e1"]["status_class"] == "5xx"
    assert by_id["e1"]["is_error"] is True
    assert by_id["e2"]["status_class"] == "4xx"
    assert by_id["e2"]["is_error"] is False


def test_transform_is_idempotent(spark: SparkSession, tmp_path: Path) -> None:
    records = [_app_event(f"e{i}") for i in range(50)]
    raw = tmp_path / "raw"
    _write_records(raw, records)
    first = run_transform(spark, str(raw), str(tmp_path / "c1"),
                          str(tmp_path / "q1"), "app_log")
    second = run_transform(spark, str(raw), str(tmp_path / "c2"),
                           str(tmp_path / "q2"), "app_log")
    assert first["rows_out"] == second["rows_out"]
    assert first["duplicates_removed"] == second["duplicates_removed"]
    count1 = spark.read.parquet(str(tmp_path / "c1")).count()
    count2 = spark.read.parquet(str(tmp_path / "c2")).count()
    assert count1 == count2 == first["rows_out"]


def test_lag_percentiles_present(spark: SparkSession, tmp_path: Path) -> None:
    records = [_app_event(f"e{i}", ingest_offset_seconds=i * 10) for i in range(100)]
    report = _run(spark, tmp_path, records)
    assert report["ingest_lag_p50"] <= report["ingest_lag_p95"] <= report["ingest_lag_p99"]


def test_clickstream_cleanup(spark: SparkSession, tmp_path: Path) -> None:
    records = [
        {
            "source": "clickstream",
            "event_id": "c1",
            "event_time": BASE.isoformat(),
            "session_id": "s1",
            "user_id": "u1",
            "page": "/HOME",
            "referrer": "direct",
            "device": "MOBILE",
            "country": "us",
            "action": "view",
            "dwell_ms": 100,
            "experiment_variant": "A",
            "ingest_time": (BASE + timedelta(seconds=5)).isoformat(),
        }
    ]
    raw = tmp_path / "raw"
    _write_records(raw, records)
    run_transform(spark, str(raw), str(tmp_path / "curated"),
                  str(tmp_path / "quarantine"), "clickstream")
    row = spark.read.parquet(str(tmp_path / "curated")).collect()[0]
    assert row["device"] == "mobile"
    assert row["country"] == "US"
    assert row["ingest_lag_seconds"] == 5
    assert str(row["dt"]) == "2024-06-01"


def test_file_count_is_controlled(spark: SparkSession, tmp_path: Path) -> None:
    records = [_app_event(f"e{i}", offset_minutes=i % 180) for i in range(300)]
    raw = tmp_path / "raw"
    _write_records(raw, records)
    report = run_transform(spark, str(raw), str(tmp_path / "curated"),
                           str(tmp_path / "quarantine"), "app_log", output_partitions=2)
    files = list((tmp_path / "curated").rglob("*.parquet"))
    assert len(files) <= report["output_partitions"] * report["distinct_dt"]
    assert len(files) >= report["distinct_dt"]
