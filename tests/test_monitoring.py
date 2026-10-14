"""Tests for pipeline metrics, alarms and the incident simulator."""

from __future__ import annotations

import pytest

from logwarehouse.aws import reset_clients
from logwarehouse.config import get_settings
from monitoring import alarms, metrics, simulate_incident


def _healthy() -> dict:
    return {
        "freshness_minutes": 10.0,
        "total_events": 193143,
        "events_last_hour": 5000,
        "events_last_hour_by_source": {"app_log": 2500, "clickstream": 2500},
        "trailing_hourly_avg": 4800.0,
        "events_last_hour_drop_pct": -4.0,
        "ingest_lag_p50": 45.0,
        "ingest_lag_p95": 90.0,
        "ingest_lag_p99": 2500.0,
        "error_rate_overall": 4.0,
        "error_rate_by_service": {"checkout": 11.0, "auth": 1.8},
        "quarantine_rate": 1.0,
        "duplicate_rate": 2.0,
        "incident_active": False,
    }


def test_alarm_specs_derive_thresholds_from_settings() -> None:
    settings = get_settings()
    specs = {s.name: s for s in alarms.alarm_specs(settings)}
    assert specs["logwarehouse-freshness"].threshold == settings.freshness_threshold_minutes
    assert specs["logwarehouse-error-rate"].threshold == settings.error_rate_threshold * 100
    assert specs["logwarehouse-quarantine-rate"].threshold == settings.quarantine_rate_threshold * 100
    assert len(specs) == 5


def test_evaluate_healthy_has_no_breaches() -> None:
    results = alarms.evaluate(_healthy())
    assert not any(r["breached"] for r in results)
    assert len(results) == 5


def test_evaluate_incident_breaches_all() -> None:
    settings = get_settings()
    degraded = simulate_incident.build_degraded_metrics(settings, _healthy())
    results = alarms.evaluate(degraded, settings)
    assert sum(r["breached"] for r in results) == 5


def test_degraded_metrics_reflect_stall_and_spike() -> None:
    settings = get_settings()
    degraded = simulate_incident.build_degraded_metrics(settings, _healthy())
    assert degraded["events_last_hour"] == 0
    assert degraded["events_last_hour_drop_pct"] == 100.0
    assert degraded["freshness_minutes"] > settings.freshness_threshold_minutes
    assert degraded["error_rate_overall"] > settings.error_rate_threshold * 100
    assert degraded["quarantine_rate"] > settings.quarantine_rate_threshold * 100


class _FakeCloudWatch:
    def __init__(self) -> None:
        self.calls: list[list[dict]] = []

    def put_metric_data(self, *, Namespace: str, MetricData: list[dict]) -> None:
        self.calls.append(MetricData)


def test_publish_metrics_builds_datapoints(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_clients()
    fake = _FakeCloudWatch()
    monkeypatch.setattr(metrics, "client", lambda *a, **k: fake)
    count = metrics.publish_metrics(_healthy())
    datapoints = [d for call in fake.calls for d in call]
    names = {d["MetricName"] for d in datapoints}
    assert count == len(datapoints)
    assert {"PipelineFreshnessMinutes", "ErrorRate", "QuarantineRate", "EventsLastHour"} <= names
    # per-service error rate carries a Service dimension
    assert any(
        d.get("Dimensions", [{}])[0].get("Name") == "Service" for d in datapoints
    )


class _RaisingCloudWatch:
    def describe_alarms(self, **kwargs):
        from botocore.exceptions import ClientError

        raise ClientError({"Error": {"Code": "InternalFailure"}}, "DescribeAlarms")


def test_describe_alarm_states_is_graceful(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_clients()
    monkeypatch.setattr(alarms, "client", lambda *a, **k: _RaisingCloudWatch())
    assert alarms.describe_alarm_states() == {}


@pytest.mark.integration
def test_compute_metrics_against_real_warehouse() -> None:
    settings = get_settings()
    if not settings.duckdb_path.exists():
        pytest.skip("warehouse.duckdb not built")
    snapshot = metrics.compute_metrics(settings)
    assert snapshot["total_events"] > 0
    assert snapshot["ingest_lag_p50"] <= snapshot["ingest_lag_p95"] <= snapshot["ingest_lag_p99"]
    assert set(snapshot["error_rate_by_service"])
