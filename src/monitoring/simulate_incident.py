"""Replay a degraded pipeline window and prove the alarms fire.

The scenario is a **stalled producer + error spike**:

* the newest event stops arriving, so freshness climbs and the hourly row count
  collapses (``EventsLastHour = 0``, 100% drop vs the trailing average);
* one service starts returning 5xx, so the error rate spikes;
* malformed records surge, so the quarantine rate spikes.

The degraded metric values are published to CloudWatch; the alarms genuinely
transition to ``ALARM`` and the resulting states are captured in
``artifacts/alarm_evidence.json``. ``--restore`` clears the incident.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from typing import Any

from logwarehouse.aws import client
from logwarehouse.config import Settings, get_settings
from monitoring.alarms import alarm_specs, describe_alarm_states, ensure_alarms, evaluate
from monitoring.metrics import compute_metrics, publish_metrics

INCIDENT_FILE = "incident_active.json"
EVIDENCE_FILE = "alarm_evidence.json"


def _incident_path(settings: Settings):
    return settings.artifacts_dir / INCIDENT_FILE


def build_degraded_metrics(settings: Settings, healthy: dict[str, Any]) -> dict[str, Any]:
    """Apply the incident transforms to a healthy metric snapshot."""
    degraded = dict(healthy)
    degraded.update(
        {
            "incident_active": True,
            "scenario": "stalled producer + error spike",
            "freshness_minutes": round(settings.freshness_threshold_minutes * 3.0, 2),
            "events_last_hour": 0,
            "events_last_hour_by_source": {},
            "events_last_hour_drop_pct": 100.0,
            "error_rate_overall": round(settings.error_rate_threshold * 100 * 3.0, 4),
            "error_rate_by_service": {
                **healthy["error_rate_by_service"],
                "checkout": round(settings.error_rate_threshold * 100 * 6.0, 4),
            },
            "quarantine_rate": round(settings.quarantine_rate_threshold * 100 * 3.0, 4),
        }
    )
    return degraded


def simulate(settings: Settings | None = None, wait_seconds: float = 30.0) -> dict[str, Any]:
    """Publish the degraded window, capture alarm evidence."""
    settings = settings or get_settings()
    ensure_alarms(settings)

    healthy = compute_metrics(settings)
    degraded = build_degraded_metrics(settings, healthy)
    _incident_path(settings).write_text(
        json.dumps({"active": True, "degraded": degraded}, indent=2)
    )
    publish_metrics(degraded, settings)

    expected = [spec.name for spec in alarm_specs(settings)]
    states: dict[str, str] = {}
    deadline = time.perf_counter() + wait_seconds
    while time.perf_counter() < deadline:
        states = describe_alarm_states(settings)
        if states and all(states.get(name) == "ALARM" for name in expected):
            break
        time.sleep(2.0)
    states = describe_alarm_states(settings) or states

    cloudwatch = client("cloudwatch", settings)
    try:
        reasons = {
            a["AlarmName"]: a.get("StateReason", "")
            for a in cloudwatch.describe_alarms(AlarmNames=expected).get("MetricAlarms", [])
        }
    except Exception:
        reasons = {}

    evaluator = evaluate(degraded, settings)
    observed = {r["name"]: r["observed"] for r in evaluator}
    alarms = [
        {
            "name": spec.name,
            "metric": spec.metric,
            "threshold": spec.threshold,
            "comparison": spec.comparison,
            "observed": observed.get(spec.name),
            "state": states.get(spec.name, "UNKNOWN"),
            "state_reason": reasons.get(spec.name, "")[:200],
        }
        for spec in alarm_specs(settings)
    ]
    fired = [a["name"] for a in alarms if a["state"] == "ALARM"]

    evidence = {
        "simulated_at": datetime.now(UTC).isoformat(),
        "scenario": degraded["scenario"],
        "degraded_metrics": degraded,
        "alarms": alarms,
        "fired": fired,
        "all_fired": set(fired) == set(expected),
    }
    settings.artifacts_dir.mkdir(parents=True, exist_ok=True)
    (settings.artifacts_dir / EVIDENCE_FILE).write_text(json.dumps(evidence, indent=2))
    return evidence


def restore(settings: Settings | None = None) -> None:
    """Clear the incident and republish healthy metrics."""
    settings = settings or get_settings()
    path = _incident_path(settings)
    if path.exists():
        path.unlink()
    healthy = compute_metrics(settings)
    publish_metrics(healthy, settings)
    print("incident cleared; healthy metrics republished")


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Simulate a degraded pipeline window")
    parser.add_argument("--restore", action="store_true", help="clear the incident")
    args = parser.parse_args(argv)
    if args.restore:
        restore()
        return 0
    evidence = simulate()
    print(json.dumps({k: evidence[k] for k in ("scenario", "fired", "all_fired")}, indent=2))
    return 0 if evidence["all_fired"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
