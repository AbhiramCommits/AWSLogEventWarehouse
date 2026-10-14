"""Before/after SQL tuning benchmark.

Two physical representations of the same denormalised fact are built from the
DuckDB warehouse:

* **baseline** — one uncompressed CSV file, no sort order, no partition pruning;
  queries use ``SELECT *``-style joins and read every row.
* **tuned** — Snappy Parquet partitioned by ``dt`` and physically sorted by
  ``(service, event_ts)``; queries push the ``dt`` predicate into the scan,
  project only needed columns, and one query reads a pre-aggregated rollup.

Each query runs ``N`` times (one warm-up discarded); median and p95 wall-clock
runtimes plus scanned rows/bytes are written to
``artifacts/query_benchmark.{json,md}``.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path
from typing import Any

import duckdb

from logwarehouse.config import Settings, get_settings
from warehouse.load import connect
from warehouse.load import run as load_warehouse

RUNS = 5
BASELINE = "baseline"
TUNED = "tuned"

_TODAY = "strftime(current_date, '%Y-%m-%d')"


def _bench_dir(settings: Settings) -> Path:
    return settings.artifacts_dir / "bench"


def _wide_select() -> str:
    """Denormalised fact with dimension attributes joined in."""
    return """
        SELECT f.event_sk, f.event_id, f.event_ts, f.dt,
               CAST(strftime(f.dt, '%Y-%m-%d') AS VARCHAR) AS dt_str,
               f.hour, f.source, s.service, p.page, g.device, g.country,
               u.user_id, f.action, f.experiment_variant, f.route,
               f.status_code, f.status_class, f.latency_ms, f.dwell_ms,
               f.is_error, f.ingest_lag_seconds
        FROM fact_event_activity f
        LEFT JOIN dim_service s USING (service_sk)
        LEFT JOIN dim_page p USING (page_sk)
        LEFT JOIN dim_device_geo g USING (device_geo_sk)
        LEFT JOIN dim_user u USING (user_sk)
    """


def build_datasets(con: duckdb.DuckDBPyConnection, settings: Settings) -> dict[str, Any]:
    """Materialise the baseline CSV and tuned Parquet representations."""
    bench = _bench_dir(settings)
    base = bench / "baseline"
    tuned = bench / "tuned"
    rollup = bench / "rollup_q1"
    for path in (base, tuned, rollup):
        if path.exists():
            for child in path.rglob("*"):
                if child.is_file():
                    child.unlink()
        path.mkdir(parents=True, exist_ok=True)

    wide = _wide_select()
    # BASELINE: single uncompressed CSV, insertion order (no sort).
    con.execute(
        f"COPY ({wide}) TO '{base / 'events.csv'}' (HEADER, DELIMITER ',')"
    )
    # Small dim for the baseline join.
    con.execute(
        f"COPY (SELECT service FROM dim_service) TO '{base / 'dim_service.csv'}' (HEADER)"
    )
    # TUNED: Snappy Parquet partitioned by dt, physically sorted by (service, event_ts).
    con.execute(
        f"COPY (SELECT * FROM ({wide}) ORDER BY service NULLS LAST, event_ts) "
        f"TO '{tuned}' (FORMAT PARQUET, COMPRESSION SNAPPY, PARTITION_BY (dt_str), "
        f"OVERWRITE_OR_IGNORE)"
    )
    # Pre-aggregated rollup for q1 (justified: repeated hourly error-rate scans).
    con.execute(
        f"""
        COPY (
            SELECT dt_str, hour, service, count(*) AS requests,
                   sum(CASE WHEN is_error THEN 1 ELSE 0 END) AS errors
            FROM ({wide}) WHERE source = 'app_log'
            GROUP BY dt_str, hour, service
        ) TO '{rollup}' (FORMAT PARQUET, COMPRESSION SNAPPY,
                         PARTITION_BY (dt_str), OVERWRITE_OR_IGNORE)
        """
    )

    sizes = {
        "baseline_csv_bytes": (base / "events.csv").stat().st_size
        + (base / "dim_service.csv").stat().st_size,
        "tuned_parquet_bytes": sum(p.stat().st_size for p in tuned.rglob("*.parquet")),
        "rollup_parquet_bytes": sum(p.stat().st_size for p in rollup.rglob("*.parquet")),
    }
    sizes["baseline_rows"] = con.execute(f"SELECT count(*) FROM ({wide})").fetchone()[0]
    return sizes


def _queries() -> dict[str, dict[str, str]]:
    """Return the baseline and tuned SQL for each analytical query."""
    return {
        "q1_hourly_error_rate_by_service": {
            BASELINE: f"""
                SELECT b.dt_str AS dt, b.hour, b.service, count(*) AS requests,
                       sum(CASE WHEN b.is_error THEN 1 ELSE 0 END) AS errors
                FROM read_csv_auto('{{base}}/events.csv') b
                JOIN read_csv_auto('{{base}}/dim_service.csv') s ON b.service = s.service
                WHERE b.source = 'app_log' AND CAST(b.dt_str AS VARCHAR) >= {_TODAY}
                GROUP BY b.dt_str, b.hour, b.service
                ORDER BY dt, hour, service
            """,
            TUNED: """
                SELECT dt_str AS dt, hour, service, requests, errors
                FROM read_parquet('{rollup}/dt_str=*/*.parquet', hive_partitioning=1)
                WHERE CAST(dt_str AS VARCHAR) >= strftime(current_date, '%Y-%m-%d')
                ORDER BY dt, hour, service
            """,
        },
        "q2_funnel_conversion_by_variant": {
            BASELINE: f"""
                SELECT b.experiment_variant AS variant,
                       count(DISTINCT CASE WHEN b.action = 'view' THEN b.event_id END) AS viewers,
                       count(DISTINCT CASE WHEN b.action = 'click' THEN b.event_id END) AS clickers,
                       count(DISTINCT CASE WHEN b.action = 'add_to_cart' THEN b.event_id END) AS carters,
                       count(DISTINCT CASE WHEN b.action = 'purchase' THEN b.event_id END) AS purchasers
                FROM read_csv_auto('{{base}}/events.csv') b
                WHERE b.source = 'clickstream' AND CAST(b.dt_str AS VARCHAR) >= {_TODAY}
                GROUP BY b.experiment_variant
                ORDER BY variant
            """,
            TUNED: """
                SELECT experiment_variant AS variant,
                       count(DISTINCT CASE WHEN action = 'view' THEN event_id END) AS viewers,
                       count(DISTINCT CASE WHEN action = 'click' THEN event_id END) AS clickers,
                       count(DISTINCT CASE WHEN action = 'add_to_cart' THEN event_id END) AS carters,
                       count(DISTINCT CASE WHEN action = 'purchase' THEN event_id END) AS purchasers
                FROM read_parquet('{tuned}/dt_str=*/*.parquet', hive_partitioning=1)
                WHERE source = 'clickstream' AND CAST(dt_str AS VARCHAR) >= strftime(current_date, '%Y-%m-%d')
                GROUP BY experiment_variant
                ORDER BY variant
            """,
        },
        "q3_top_routes_p95_latency": {
            BASELINE: f"""
                SELECT b.route, count(*) AS requests,
                       quantile_cont(b.latency_ms, 0.95) AS p95_latency_ms
                FROM read_csv_auto('{{base}}/events.csv') b
                JOIN read_csv_auto('{{base}}/dim_service.csv') s ON b.service = s.service
                WHERE b.source = 'app_log' AND CAST(b.dt_str AS VARCHAR) >= {_TODAY} AND b.route IS NOT NULL
                GROUP BY b.route
                ORDER BY p95_latency_ms DESC
            """,
            TUNED: """
                SELECT route, count(*) AS requests,
                       quantile_cont(latency_ms, 0.95) AS p95_latency_ms
                FROM read_parquet('{tuned}/dt_str=*/*.parquet', hive_partitioning=1)
                WHERE source = 'app_log'
                  AND CAST(dt_str AS VARCHAR) >= strftime(current_date, '%Y-%m-%d')
                  AND route IS NOT NULL
                GROUP BY route
                ORDER BY p95_latency_ms DESC
            """,
        },
    }


def _render(sql: str, settings: Settings) -> str:
    bench = _bench_dir(settings)
    return sql.format(base=bench / "baseline", tuned=bench / "tuned", rollup=bench / "rollup_q1")


def _scanned_metrics(mode: str, name: str, settings: Settings) -> dict[str, Any]:
    """Rows/bytes the query must read given partition pruning."""
    bench = _bench_dir(settings)
    if mode == BASELINE:
        events = bench / "baseline" / "events.csv"
        dim = bench / "baseline" / "dim_service.csv"
        return {
            "files_scanned": 2,
            "rows_scanned": _csv_rows(events),
            "bytes_scanned": events.stat().st_size + dim.stat().st_size,
        }
    if name.startswith("q1"):
        files = list((bench / "rollup_q1").rglob("*.parquet"))
        return {
            "files_scanned": len(files),
            "rows_scanned": _parquet_rows(files),
            "bytes_scanned": sum(p.stat().st_size for p in files),
        }
    files = [p for p in (bench / "tuned").rglob("*.parquet") if _is_latest(p.name, p.parent.name)]
    files = files or list((bench / "tuned").rglob("*.parquet"))
    return {
        "files_scanned": len(files),
        "rows_scanned": _parquet_rows(files),
        "bytes_scanned": sum(p.stat().st_size for p in files),
    }


def _is_latest(name: str, part: str) -> bool:
    """Best-effort: keep tuned partitions whose dt_str is >= today."""
    from datetime import date

    if part.startswith("dt_str="):
        return part.split("=", 1)[1] >= date.today().isoformat()
    return True


def _csv_rows(path: Path) -> int:
    with path.open("rb") as handle:
        return max(0, sum(1 for _ in handle) - 1)


def _parquet_rows(files: list[Path]) -> int:
    if not files:
        return 0
    con = duckdb.connect()
    total = 0
    for path in files:
        total += con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]
    con.close()
    return total


def _time_query(con: duckdb.DuckDBPyConnection, sql: str) -> list[float]:
    """Run a query ``RUNS+1`` times, discarding the first, returning times."""
    con.execute(sql).fetchall()  # warm-up
    times = []
    for _ in range(RUNS):
        start = time.perf_counter()
        con.execute(sql).fetchall()
        times.append(time.perf_counter() - start)
    return times


def _result_signature(con: duckdb.DuckDBPyConnection, sql: str) -> Any:
    rows = con.execute(sql).fetchall()
    return sorted(rows, key=lambda r: str(r))


def run(load: bool = True) -> dict[str, Any]:
    """Run the benchmark and write the JSON + Markdown artifacts."""
    settings = get_settings()
    if load:
        report = load_warehouse()
        if not report["passed"]:
            raise RuntimeError(f"warehouse validation failed: {report}")

    con = connect(settings)
    try:
        con.execute("USE warehouse;")
        sizes = build_datasets(con, settings)
        queries = _queries()
        results: dict[str, Any] = {}
        for name, variants in queries.items():
            entry: dict[str, Any] = {}
            for mode in (BASELINE, TUNED):
                sql = _render(variants[mode], settings)
                times = _time_query(con, sql)
                entry[mode] = {
                    "median_seconds": round(statistics.median(times), 6),
                    "p95_seconds": round(sorted(times)[-1], 6),
                    "times": [round(t, 6) for t in times],
                    **_scanned_metrics(mode, name, settings),
                }
            b_med = entry[BASELINE]["median_seconds"]
            t_med = entry[TUNED]["median_seconds"]
            entry["speedup_median"] = round(b_med / t_med, 2) if t_med else None
            entry["rows_reduction"] = (
                round(entry[BASELINE]["rows_scanned"] / entry[TUNED]["rows_scanned"], 2)
                if entry[TUNED]["rows_scanned"]
                else None
            )
            # correctness: tuned and baseline must return the same rows
            base_sig = _result_signature(con, _render(variants[BASELINE], settings))
            tuned_sig = _result_signature(con, _render(variants[TUNED], settings))
            entry["results_match"] = base_sig == tuned_sig
            results[name] = entry
    finally:
        con.close()

    output = {"runs": RUNS, "datasets": sizes, "queries": results}
    settings.artifacts_dir.mkdir(parents=True, exist_ok=True)
    (settings.artifacts_dir / "query_benchmark.json").write_text(json.dumps(output, indent=2))
    (settings.artifacts_dir / "query_benchmark.md").write_text(_markdown(output))
    return output


def _markdown(output: dict[str, Any]) -> str:
    lines = [
        "# Query tuning benchmark",
        "",
        f"{output['runs']} measured runs per query (one warm-up discarded).",
        "",
        "| Query | Baseline median (s) | Tuned median (s) | Baseline p95 (s) | "
        "Tuned p95 (s) | Speedup (median) | Rows scanned (base → tuned) | Bytes scanned "
        "(base → tuned) | Results match |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for name, q in output["queries"].items():
        b, t = q[BASELINE], q[TUNED]
        lines.append(
            f"| {name} | {b['median_seconds']} | {t['median_seconds']} | {b['p95_seconds']} | "
            f"{t['p95_seconds']} | {q['speedup_median']}x | "
            f"{b['rows_scanned']:,} → {t['rows_scanned']:,} | "
            f"{b['bytes_scanned']:,} → {t['bytes_scanned']:,} | {q['results_match']} |"
        )
    lines.append("")
    lines.append("## Techniques credited")
    lines.append("")
    lines.extend(
        [
            "- **q1** — pre-aggregated hourly rollup Parquet replaces a full fact scan "
            "and CSV vs columnar compression.",
            "- **q2** — partitioned Snappy Parquet with `dt` predicate push-down and "
            "columnar projection (no uncompressed CSV scan).",
            "- **q3** — partitioned Snappy Parquet, `dt` pruning, physical sort on "
            "`(service, event_ts)` and narrow projection (only route/latency/error columns).",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Run the SQL tuning benchmark")
    parser.add_argument("--skip-load", action="store_true")
    args = parser.parse_args(argv)
    output = run(load=not args.skip_load)
    print(_markdown(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
