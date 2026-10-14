"""Tests for the producer schemas, generator and batching."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from producers import publish
from producers.consumer import _route
from producers.generate import DUP_RATE, MALFORMED_RATE, iter_records
from producers.schemas import (
    APP_LOG,
    CLICKSTREAM,
    partition_key,
    validate_event,
)

END = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


def _app_log() -> dict:
    return {
        "source": APP_LOG,
        "event_id": "a" * 32,
        "event_time": END.isoformat(),
        "service": "checkout",
        "host": "ip-10-0-0-1",
        "level": "INFO",
        "status_code": 200,
        "latency_ms": 42,
        "route": "/api/v1/checkout",
        "user_id": "u000001",
        "trace_id": "b" * 32,
        "message": "ok",
    }


def _clickstream() -> dict:
    return {
        "source": CLICKSTREAM,
        "event_id": "c" * 32,
        "event_time": END.isoformat(),
        "session_id": "s123",
        "user_id": "u000001",
        "page": "/home",
        "referrer": "direct",
        "device": "mobile",
        "country": "US",
        "action": "view",
        "dwell_ms": 1500,
        "experiment_variant": "A",
    }


def test_validate_app_log_ok() -> None:
    assert validate_event(APP_LOG, _app_log()) == []


def test_validate_app_log_rejects_bad_level_and_status() -> None:
    event = _app_log()
    event["level"] = "TRACE"
    event["status_code"] = 999
    errors = validate_event(APP_LOG, event)
    assert any("level" in e for e in errors)
    assert any("status_code" in e for e in errors)


def test_validate_clickstream_ok() -> None:
    assert validate_event(CLICKSTREAM, _clickstream()) == []


def test_validate_clickstream_rejects_bad_action() -> None:
    event = _clickstream()
    event["action"] = "scroll"
    assert any("action" in e for e in validate_event(CLICKSTREAM, event))


def test_null_key_is_allowed_by_schema() -> None:
    event = _clickstream()
    event["session_id"] = None
    assert validate_event(CLICKSTREAM, event) == []


def test_partition_keys() -> None:
    assert partition_key(APP_LOG, {"service": "auth"}) == "auth"
    assert partition_key(CLICKSTREAM, {"session_id": "s9"}) == "s9"
    assert partition_key(CLICKSTREAM, {"session_id": None}) == "null"


def test_generator_is_deterministic() -> None:
    first = list(iter_records(APP_LOG, 200, seed=7, end_time=END))
    second = list(iter_records(APP_LOG, 200, seed=7, end_time=END))
    assert [r.data for r in first] == [r.data for r in second]


def test_generator_seed_changes_output() -> None:
    first = list(iter_records(APP_LOG, 100, seed=1, end_time=END))
    second = list(iter_records(APP_LOG, 100, seed=2, end_time=END))
    assert [r.data for r in first] != [r.data for r in second]


def test_generator_emits_exact_count() -> None:
    assert len(list(iter_records(CLICKSTREAM, 777, seed=3, end_time=END))) == 777


def test_defect_rates_within_tolerance() -> None:
    records = list(iter_records(APP_LOG, 20_000, seed=11, end_time=END))
    dup = sum(r.is_duplicate for r in records) / len(records)
    malformed = sum(r.is_malformed for r in records) / len(records)
    null_key = sum(r.is_null_key for r in records) / len(records)
    late = sum(r.is_late for r in records) / len(records)
    assert 0.012 <= dup <= 0.028, dup
    assert 0.005 <= malformed <= 0.018, malformed
    assert 0.002 <= null_key <= 0.012, null_key
    assert 0.02 <= late <= 0.045, late
    # doubled event ids really do appear
    assert dup == pytest.approx(DUP_RATE, abs=0.01)
    assert malformed == pytest.approx(MALFORMED_RATE, abs=0.01)


def test_malformed_records_are_unparseable_but_source_recoverable() -> None:
    malformed = [r for r in iter_records(APP_LOG, 3000, seed=5, end_time=END) if r.is_malformed]
    assert malformed
    for record in malformed:
        with pytest.raises(json.JSONDecodeError):
            json.loads(record.data)
        source, _, _, ok = _route(record.data)
        assert source == APP_LOG
        assert ok is False


def test_late_events_have_larger_ingest_gap() -> None:
    records = list(iter_records(CLICKSTREAM, 5000, seed=9, end_time=END))
    late = [r for r in records if r.is_late]
    assert late
    for record in late:
        event = record.event
        et = datetime.fromisoformat(event["event_time"])
        it = datetime.fromisoformat(event["ingest_time"])
        assert it - et >= timedelta(minutes=5)


def test_null_key_records_blank_the_key() -> None:
    records = list(iter_records(CLICKSTREAM, 4000, seed=13, end_time=END))
    nulls = [r for r in records if r.is_null_key]
    assert nulls
    assert all(r.event["session_id"] is None for r in nulls)


class _FakeKinesis:
    def __init__(self) -> None:
        self.calls: list[int] = []

    def put_records(self, *, StreamName: str, Records: list[dict]) -> dict:
        self.calls.append(len(Records))
        return {"FailedRecordCount": 0, "Records": [{} for _ in Records]}


def test_batch_chunk_sizes(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeKinesis()
    monkeypatch.setattr(publish, "client", lambda *a, **k: fake)
    summary = publish.publish(APP_LOG, 1200, batch=500, seed=1)
    assert fake.calls == [500, 500, 200]
    assert summary["events"] == 1200


def test_batch_is_capped_at_500(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeKinesis()
    monkeypatch.setattr(publish, "client", lambda *a, **k: fake)
    publish.publish(CLICKSTREAM, 1000, batch=10_000, seed=1)
    assert all(size <= 500 for size in fake.calls)
