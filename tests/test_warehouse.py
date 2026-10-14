"""Tests for the dimensional model, warehouse load and query tuning.

These build a small in-memory warehouse from the same ``sql/ddl_duckdb.sql``
DDL, so they run without LocalStack. The benchmark parity test reuses the real
baseline/tuned dataset builder against that synthetic warehouse.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from logwarehouse.config import get_settings
from warehouse import benchmark

DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "ddl_duckdb.sql"
ANALYTICS_DIR = Path(__file__).resolve().parents[1] / "sql" / "analytics"
SK = "9223372036854775807"
TODAY = datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0)
YESTERDAY = TODAY - timedelta(days=1)


def _sk(expr: str) -> int:
    return duckdb.sql(f"SELECT CAST(hash({expr}) % {SK} AS BIGINT)").fetchone()[0]


@pytest.fixture()
def warehouse() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(DDL_PATH.read_text())
    con.execute("USE warehouse;")

    services = {"auth": "identity", "checkout": "commerce", "search": "discovery"}
    for svc, grp in services.items():
        con.execute(
            "INSERT INTO dim_service VALUES (?,?,?)", [_sk(repr(svc)), svc, grp]
        )
    pages = {"/home": "home", "/checkout": "checkout"}
    for page, ptype in pages.items():
        con.execute("INSERT INTO dim_page VALUES (?,?,?)", [_sk(repr(page)), page, ptype])
    devices = {("mobile", "US"): "Americas", ("desktop", "DE"): "EMEA"}
    for (dev, ctry), region in devices.items():
        con.execute(
            "INSERT INTO dim_device_geo VALUES (?,?,?,?)",
            [_sk(f"'{dev}' || '|' || '{ctry}'"), dev, ctry, region],
        )
    users = ["u1", "u2", "u3"]
    for u in users:
        con.execute(
            "INSERT INTO dim_user VALUES (?,?,?,?,?)",
            [_sk(repr(u)), u, TODAY, TODAY, True],
        )

    rows = []
    for i in range(120):
        ts = (TODAY if i % 2 == 0 else YESTERDAY) + timedelta(minutes=i)
        svc = list(services)[i % 3]
        rows.append(
            (
                _sk(f"'app_log' || '|' || 'ae{i}'"), f"ae{i}", ts, ts.date(), ts.hour,
                "app_log", _sk(repr(svc)), None, _sk(repr("u1")), None, None, None,
                f"/api/{svc}/x", 500 if i % 10 == 0 else 200,
                "5xx" if i % 10 == 0 else "2xx", 100 + i, None, i % 10 == 0, i,
            )
        )
    for i in range(60):
        ts = (TODAY if i % 2 == 0 else YESTERDAY) + timedelta(minutes=i)
        page = list(pages)[i % 2]
        dev, ctry = list(devices)[i % 2]
        rows.append(
            (
                _sk(f"'clickstream' || '|' || 'ce{i}'"), f"ce{i}", ts, ts.date(), ts.hour,
                "clickstream", None, _sk(repr(page)), _sk(repr("u1")),
                _sk(f"'{dev}' || '|' || '{ctry}'"), ["view", "click", "purchase"][i % 3],
                ["control", "A", "B"][i % 3], None, None, None, None, 50 + i, False, i,
            )
        )
    con.executemany(
        "INSERT INTO fact_event_activity VALUES (" + ",".join(["?"] * 19) + ")", rows
    )
    yield con
    con.close()


def test_fact_row_count_parity(warehouse: duckdb.DuckDBPyConnection) -> None:
    fact = warehouse.execute("SELECT count(*) FROM fact_event_activity").fetchone()[0]
    distinct = warehouse.execute(
        "SELECT count(DISTINCT event_sk) FROM fact_event_activity"
    ).fetchone()[0]
    assert fact == 180
    assert fact == distinct


def test_no_duplicate_surrogate_keys(warehouse: duckdb.DuckDBPyConnection) -> None:
    for table, key in [
        ("fact_event_activity", "event_sk"),
        ("dim_user", "user_sk"),
        ("dim_service", "service_sk"),
        ("dim_page", "page_sk"),
        ("dim_device_geo", "device_geo_sk"),
    ]:
        total, distinct = warehouse.execute(
            f"SELECT count(*), count(DISTINCT {key}) FROM {table}"
        ).fetchone()
        assert total == distinct, table


def test_no_orphan_foreign_keys(warehouse: duckdb.DuckDBPyConnection) -> None:
    checks = {
        "service": "LEFT JOIN dim_service d USING (service_sk) WHERE f.service_sk IS NOT NULL AND d.service_sk IS NULL",
        "page": "LEFT JOIN dim_page d USING (page_sk) WHERE f.page_sk IS NOT NULL AND d.page_sk IS NULL",
        "user": "LEFT JOIN dim_user d USING (user_sk) WHERE f.user_sk IS NOT NULL AND d.user_sk IS NULL",
        "device": "LEFT JOIN dim_device_geo d USING (device_geo_sk) WHERE f.device_geo_sk IS NOT NULL AND d.device_geo_sk IS NULL",
    }
    for name, join in checks.items():
        orphan = warehouse.execute(
            f"SELECT count(*) FROM fact_event_activity f {join}"
        ).fetchone()[0]
        assert orphan == 0, f"orphan {name} keys"


def test_orphan_detection_would_catch_bad_rows(warehouse: duckdb.DuckDBPyConnection) -> None:
    warehouse.execute(
        "INSERT INTO fact_event_activity (event_sk, event_id, event_ts, dt, hour, source, "
        "service_sk, is_error) VALUES (999, 'bad', now(), current_date, 1, 'app_log', -1, false)"
    )
    orphan = warehouse.execute(
        "SELECT count(*) FROM fact_event_activity f LEFT JOIN dim_service d USING (service_sk) "
        "WHERE f.service_sk IS NOT NULL AND d.service_sk IS NULL"
    ).fetchone()[0]
    assert orphan == 1


def test_analytics_queries_execute(warehouse: duckdb.DuckDBPyConnection) -> None:
    for path in sorted(ANALYTICS_DIR.glob("q*.sql")):
        rows = warehouse.execute(path.read_text()).fetchall()
        assert rows, f"{path.name} returned no rows"


def _tmp_settings(tmp_path: Path):
    return dataclasses.replace(get_settings(), artifacts_dir=tmp_path)


def _signature(con: duckdb.DuckDBPyConnection, sql: str):
    return sorted(con.execute(sql).fetchall(), key=lambda r: str(r))


def test_tuned_matches_baseline_for_all_queries(
    warehouse: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    settings = _tmp_settings(tmp_path)
    benchmark.build_datasets(warehouse, settings)
    queries = benchmark._queries()
    for name, variants in queries.items():
        base = benchmark._render(variants[benchmark.BASELINE], settings)
        tuned = benchmark._render(variants[benchmark.TUNED], settings)
        assert _signature(warehouse, base) == _signature(warehouse, tuned), name


def test_tuned_scans_no_more_than_baseline(
    warehouse: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    settings = _tmp_settings(tmp_path)
    sizes = benchmark.build_datasets(warehouse, settings)
    assert sizes["tuned_parquet_bytes"] <= sizes["baseline_csv_bytes"]
