#!/usr/bin/env python3
"""End-to-end pipeline orchestration.

Runs, with structured logging and per-stage timing::

    bootstrap -> produce -> deliver -> transform -> catalog -> load
              -> benchmark -> metrics/alarms -> incident replay

Every stage is retried with backoff, re-runs are made idempotent by resetting
local state first, and ``--fail-stage`` injects a deliberate failure for
runbook drills. A final ``artifacts/pipeline_run.json`` collects every headline
number pulled from the stage artifacts.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from glue import catalog  # noqa: E402
from glue import run as glue_run
from logwarehouse.config import get_settings  # noqa: E402
from monitoring import alarms, metrics, simulate_incident  # noqa: E402
from producers import consumer as delivery  # noqa: E402
from producers import publish  # noqa: E402
from warehouse import benchmark, load  # noqa: E402

STAGES = ["bootstrap", "produce", "deliver", "transform", "catalog", "load",
          "benchmark", "monitor", "incident"]


def log(stage: str, message: str, **fields: Any) -> None:
    """Emit a structured key=value log line."""
    extra = " ".join(f"{k}={v}" for k, v in fields.items())
    print(f"stage={stage} {message} {extra}".rstrip(), flush=True)


def _subprocess(script: str, *args: str) -> None:
    subprocess.run([sys.executable, str(ROOT / "scripts" / script), *args], check=True, cwd=ROOT)


def _json(name: str) -> dict[str, Any]:
    path = get_settings().artifacts_dir / name
    return json.loads(path.read_text()) if path.exists() else {}


def run_stage(name: str, fn: Callable[[], Any], fail_stage: str | None, retries: int = 3) -> Any:
    """Run one stage with retries/backoff and timing; honour ``--fail-stage``."""
    log(name, "start")
    start = time.perf_counter()
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            result = fn()
            if fail_stage == name:
                raise RuntimeError(f"injected failure at stage '{name}'")
            duration = time.perf_counter() - start
            log(name, "done", seconds=round(duration, 3), attempts=attempt)
            return result
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if fail_stage == name:
                log(name, "injected failure", error=str(exc))
                raise
            log(name, "retry", attempt=attempt, error=str(exc)[:200])
            time.sleep(min(2**attempt, 8))
    raise RuntimeError(f"stage '{name}' failed after {retries} attempts: {last_error}")


def build_orchestration(args: argparse.Namespace) -> dict[str, Callable[[], Any]]:
    """Return the stage name -> callable mapping."""
    settings = get_settings()
    app_events = args.app_events if args.app_events else settings.app_log_events
    click_events = args.click_events if args.click_events else settings.clickstream_events

    def produce() -> dict:
        a = publish.publish("app_log", app_events)
        publish._write_manifest(a)
        c = publish.publish("clickstream", click_events)
        publish._write_manifest(c)
        return {"app_log": a, "clickstream": c}

    def bootstrap() -> None:
        if args.reset:
            _subprocess("reset_state.py")
        _subprocess("bootstrap_local_aws.py")

    def incident() -> dict:
        evidence = simulate_incident.simulate()
        simulate_incident.restore()
        return evidence

    return {
        "bootstrap": bootstrap,
        "produce": produce,
        "deliver": delivery.deliver_all,
        "transform": lambda: glue_run.run(["app_log", "clickstream"]),
        "catalog": lambda: catalog.run(["app_log", "clickstream"]),
        "load": load.run,
        "benchmark": benchmark.run,
        "monitor": _monitor,
        "incident": incident,
    }


def _monitor() -> dict[str, Any]:
    snapshot = metrics.run()
    created = alarms.ensure_alarms()
    exit_code = alarms.run_check()
    return {"metrics": snapshot, "alarms_created": len(created), "healthy_check_exit": exit_code}


def _apply_small_thresholds() -> None:
    """Relax alarm thresholds so the tiny CI corpus is still 'healthy'."""
    import os

    os.environ.setdefault("ROWCOUNT_FLOOR", "1")
    os.environ.setdefault("ROWCOUNT_DROP_PCT", "100")
    os.environ.setdefault("ERROR_RATE_THRESHOLD", "0.10")
    os.environ.setdefault("QUARANTINE_RATE_THRESHOLD", "0.05")
    os.environ.setdefault("FRESHNESS_THRESHOLD_MINUTES", "1440")
    get_settings(reload=True)


def collect_headline(args: argparse.Namespace) -> dict[str, Any]:
    """Assemble the headline numbers from the stage artifacts."""
    settings = get_settings()
    ingest = _json("ingest_report.json")
    transform = _json("transform_report.json")
    warehouse = _json("warehouse_report.json")
    bench = _json("query_benchmark.json")
    snapshot = _json("metrics_snapshot.json")
    evidence = _json("alarm_evidence.json")
    catalog_summary = _json("catalog_summary.json")
    return {
        "corpus": {
            "app_log_events": args.app_events or settings.app_log_events,
            "clickstream_events": args.click_events or settings.clickstream_events,
            "total_events": (args.app_events or settings.app_log_events)
            + (args.click_events or settings.clickstream_events),
        },
        "ingest": {
            "total_events": ingest.get("total_events"),
            "raw_bytes": ingest.get("raw_bytes"),
            "s3_objects": ingest.get("s3_objects"),
            "partitions": ingest.get("partitions"),
            "records_per_sec": ingest.get("records_per_sec"),
            "malformed_records": ingest.get("malformed_records"),
            "delivery_path": ingest.get("delivery_path"),
        },
        "transform": transform.get("totals", {}),
        "warehouse": warehouse,
        "query_benchmark": {
            name: {
                "baseline_median_s": q["baseline"]["median_seconds"],
                "tuned_median_s": q["tuned"]["median_seconds"],
                "speedup_median": q["speedup_median"],
                "results_match": q["results_match"],
            }
            for name, q in bench.get("queries", {}).items()
        },
        "metrics": snapshot,
        "alarms_fired": evidence.get("fired", []),
        "catalog_tables": list(catalog_summary.get("tables", {}).keys()),
    }


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Run the end-to-end pipeline")
    parser.add_argument("--app-events", type=int, default=None)
    parser.add_argument("--click-events", type=int, default=None)
    parser.add_argument("--skip-bootstrap", action="store_true")
    parser.add_argument("--reset", action="store_true", help="reset local state before running")
    parser.add_argument("--fail-stage", choices=STAGES, default=None)
    parser.add_argument("--small", action="store_true", help="tiny corpus (CI smoke test)")
    args = parser.parse_args(argv)
    if args.small:
        args.app_events = args.app_events or 2000
        args.click_events = args.click_events or 2000
        args.reset = True
        _apply_small_thresholds()

    stages = build_orchestration(args)
    order = [s for s in STAGES if s != "bootstrap" or not args.skip_bootstrap]
    # The incident replay is a demonstration; skip it in small/smoke runs.
    if args.small:
        order = [s for s in order if s != "incident"]

    timings: dict[str, float] = {}
    outputs: dict[str, Any] = {}
    total_start = time.perf_counter()
    try:
        for stage in order:
            start = time.perf_counter()
            outputs[stage] = run_stage(stage, stages[stage], args.fail_stage)
            timings[stage] = round(time.perf_counter() - start, 3)
    except Exception as exc:  # noqa: BLE001
        log("pipeline", "failed", error=str(exc)[:300])
        return 1
    total = time.perf_counter() - total_start

    headline = collect_headline(args)
    output = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "stage_seconds": timings,
        "total_wall_seconds": round(total, 3),
        "headline": headline,
    }
    settings = get_settings()
    (settings.artifacts_dir / "pipeline_run.json").write_text(json.dumps(output, indent=2))
    log("pipeline", "done", total_seconds=round(total, 3),
        stages=len(timings))
    print(json.dumps(output, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
