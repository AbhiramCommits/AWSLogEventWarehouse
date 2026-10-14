"""End-to-end smoke test for the pipeline orchestrator.

Marked ``integration`` because it launches the small corpus through the real
LocalStack + moto stack. Run it with ``pytest -m integration``. Artifacts are
written to a temporary directory so the checked-in reports are untouched.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _run_env(artifacts: Path) -> dict[str, str]:
    env = dict(os.environ)
    env.setdefault("AWS_ACCESS_KEY_ID", "test")
    env.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    env.setdefault("AWS_REGION", "us-east-1")
    env["ARTIFACTS_DIR"] = str(artifacts)
    env["DUCKDB_PATH"] = str(artifacts / "warehouse.duckdb")
    return env


def _run_pipeline(artifacts: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "run_pipeline.py"), *args],
        cwd=ROOT, env=_run_env(artifacts), capture_output=True, text=True, timeout=900,
    )


@pytest.mark.integration
def test_small_pipeline_end_to_end(tmp_path: Path) -> None:
    result = _run_pipeline(tmp_path, "--small")
    assert result.returncode == 0, result.stderr[-2000:]
    report = json.loads((tmp_path / "pipeline_run.json").read_text())
    assert report["headline"]["ingest"]["total_events"] == 4000
    assert report["headline"]["transform"]["rows_in"] == 4000
    assert report["headline"]["warehouse"]["fact_matches_curated"] is True
    assert report["total_wall_seconds"] > 0


@pytest.mark.integration
def test_failure_injection_returns_nonzero(tmp_path: Path) -> None:
    result = _run_pipeline(tmp_path, "--small", "--fail-stage", "transform")
    assert result.returncode == 1
    assert "injected failure at stage 'transform'" in result.stdout + result.stderr


def test_run_pipeline_stage_list_is_complete() -> None:
    sys.path.insert(0, str(ROOT / "scripts"))
    import run_pipeline

    assert run_pipeline.STAGES == [
        "bootstrap", "produce", "deliver", "transform", "catalog", "load",
        "benchmark", "monitor", "incident",
    ]
