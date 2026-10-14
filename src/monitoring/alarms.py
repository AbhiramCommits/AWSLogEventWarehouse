"""CloudWatch data-quality alarms and a CI gate.

Alarms created on the ``LogWarehouse/Pipeline`` namespace (all wired to the
``pipeline-alarms`` SNS topic):

* ``logwarehouse-freshness``    — ``PipelineFreshnessMinutes`` > threshold
* ``logwarehouse-rowcount-floor`` — ``EventsLastHour`` < floor
* ``logwarehouse-rowcount-drop``  — ``EventsLastHourDropPct`` > threshold
* ``logwarehouse-error-rate``     — ``ErrorRate`` > threshold
* ``logwarehouse-quarantine-rate``— ``QuarantineRate`` > threshold

``--check`` evaluates the same conditions against the current metric snapshot
and exits non-zero on any breach, so it can run as a data-quality gate in CI.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any

from botocore.exceptions import ClientError

from logwarehouse.aws import client
from logwarehouse.config import Settings, get_settings
from monitoring.metrics import NAMESPACE, compute_metrics


@dataclass(frozen=True)
class AlarmSpec:
    """A single alarm definition tied to a metric and a comparison."""

    name: str
    metric: str
    threshold: float
    comparison: str
    unit: str
    description: str


def alarm_specs(settings: Settings) -> list[AlarmSpec]:
    """Build the alarm definitions from the configured thresholds."""
    return [
        AlarmSpec(
            "logwarehouse-freshness", "PipelineFreshnessMinutes",
            float(settings.freshness_threshold_minutes), "GreaterThanThreshold", "Minutes",
            "Newest curated event is older than the freshness threshold.",
        ),
        AlarmSpec(
            "logwarehouse-rowcount-floor", "EventsLastHour",
            float(settings.rowcount_floor), "LessThanThreshold", "Count",
            "Hourly event count fell below the floor (producer stalled).",
        ),
        AlarmSpec(
            "logwarehouse-rowcount-drop", "EventsLastHourDropPct",
            float(settings.rowcount_drop_pct), "GreaterThanThreshold", "Percent",
            "Hourly event count dropped more than X% vs the trailing average.",
        ),
        AlarmSpec(
            "logwarehouse-error-rate", "ErrorRate",
            float(settings.error_rate_threshold * 100), "GreaterThanThreshold", "Percent",
            "Application error rate above threshold.",
        ),
        AlarmSpec(
            "logwarehouse-quarantine-rate", "QuarantineRate",
            float(settings.quarantine_rate_threshold * 100), "GreaterThanThreshold", "Percent",
            "Quarantine (unparseable) rate above threshold.",
        ),
    ]


def topic_arn(settings: Settings) -> str:
    """Resolve the SNS topic ARN for alarm actions."""
    sns = client("sns", settings)
    topics = sns.list_topics()["Topics"]
    for topic in topics:
        if topic["TopicArn"].endswith(f":{settings.alarm_topic}"):
            return topic["TopicArn"]
    return f"arn:aws:sns:{settings.region}:000000000000:{settings.alarm_topic}"


def ensure_alarms(settings: Settings | None = None) -> list[dict[str, Any]]:
    """Create/update every alarm and return their definitions."""
    settings = settings or get_settings()
    cloudwatch = client("cloudwatch", settings)
    arn = topic_arn(settings)
    created = []
    for spec in alarm_specs(settings):
        cloudwatch.put_metric_alarm(
            AlarmName=spec.name,
            AlarmDescription=spec.description,
            Namespace=NAMESPACE,
            MetricName=spec.metric,
            Statistic="Maximum" if spec.metric != "EventsLastHour" else "Minimum",
            Period=300,
            EvaluationPeriods=1,
            ComparisonOperator=spec.comparison,
            Threshold=spec.threshold,
            TreatMissingData="breaching",
            ActionsEnabled=True,
            AlarmActions=[arn],
            Unit=spec.unit,
        )
        created.append(
            {"name": spec.name, "metric": spec.metric, "threshold": spec.threshold,
             "comparison": spec.comparison}
        )
    return created


def evaluate(metrics: dict[str, Any], settings: Settings | None = None) -> list[dict[str, Any]]:
    """Evaluate every alarm condition against a metric snapshot."""
    settings = settings or get_settings()
    values = {
        "PipelineFreshnessMinutes": metrics["freshness_minutes"],
        "EventsLastHour": metrics["events_last_hour"],
        "EventsLastHourDropPct": metrics["events_last_hour_drop_pct"],
        "ErrorRate": metrics["error_rate_overall"],
        "QuarantineRate": metrics["quarantine_rate"],
    }
    results = []
    for spec in alarm_specs(settings):
        observed = float(values[spec.metric])
        if spec.comparison == "GreaterThanThreshold":
            breached = observed > spec.threshold
        else:
            breached = observed < spec.threshold
        results.append(
            {"name": spec.name, "metric": spec.metric, "threshold": spec.threshold,
             "comparison": spec.comparison, "observed": observed, "breached": breached}
        )
    return results


def describe_alarm_states(settings: Settings | None = None) -> dict[str, str]:
    """Best-effort read of the current CloudWatch alarm states."""
    settings = settings or get_settings()
    cloudwatch = client("cloudwatch", settings)
    names = [spec.name for spec in alarm_specs(settings)]
    try:
        alarms = cloudwatch.describe_alarms(AlarmNames=names).get("MetricAlarms", [])
        return {a["AlarmName"]: a["StateValue"] for a in alarms}
    except ClientError:
        return {}


def run_check(as_json: bool = False) -> int:
    """Compute metrics and evaluate the gate; return 1 on any breach."""
    settings = get_settings()
    metrics = compute_metrics(settings)
    results = evaluate(metrics, settings)
    breached = [r for r in results if r["breached"]]
    if as_json:
        print(json.dumps({"results": results, "breached": [r["name"] for r in breached]}, indent=2))
    else:
        for r in results:
            flag = "BREACH" if r["breached"] else "ok"
            print(f"[{flag}] {r['name']}: {r['metric']}={r['observed']} "
                  f"{r['comparison']} {r['threshold']}")
    return 1 if breached else 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point: create alarms unless ``--check`` is passed."""
    parser = argparse.ArgumentParser(description="CloudWatch pipeline alarms / CI gate")
    parser.add_argument("--check", action="store_true", help="evaluate and exit non-zero on breach")
    parser.add_argument("--json", action="store_true", help="emit check results as JSON")
    args = parser.parse_args(argv)

    if args.check:
        return run_check(as_json=args.json)

    settings = get_settings()
    created = ensure_alarms(settings)
    print(json.dumps({"created": created, "topic": topic_arn(settings)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
