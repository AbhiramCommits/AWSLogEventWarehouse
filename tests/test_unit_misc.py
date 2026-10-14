"""Additional unit tests for module-level helpers (no AWS required)."""

from __future__ import annotations

from datetime import UTC, datetime

from glue import catalog
from glue.transform_job import APP_LOG_SCHEMA, build_glue_context, raw_schema
from logwarehouse.aws import _endpoint_for
from logwarehouse.config import get_settings
from producers.consumer import Buffer, _route
from producers.schemas import APP_LOG, CLICKSTREAM


def test_config_s3_uri() -> None:
    settings = get_settings()
    assert settings.s3_uri("b", "a", "b") == "s3://b/a/b"
    assert settings.raw_uri.endswith(settings.raw_prefix)


def test_aws_endpoint_overrides() -> None:
    settings = get_settings()
    assert _endpoint_for("kinesis", settings) == settings.kinesis_endpoint_url
    assert _endpoint_for("s3", settings) == settings.endpoint_url


def test_transform_schemas_include_corrupt_record() -> None:
    for source in (APP_LOG, CLICKSTREAM):
        names = [f.name for f in raw_schema(source).fields]
        assert "_corrupt_record" in names
    assert [f.name for f in APP_LOG_SCHEMA.fields][0] == "event_id"


def test_build_glue_context_returns_none_off_glue() -> None:
    assert build_glue_context({}) is None


def test_catalog_schemas_and_location() -> None:
    settings = get_settings()
    assert catalog.table_name("app_log") == "curated_app_log"
    assert catalog.curated_location(settings, "app_log").endswith("source=app_log")
    table = catalog._table_input(settings, "clickstream")
    assert table["PartitionKeys"] == [{"Name": "dt", "Type": "string"}]
    assert any(c["Name"] == "session_id" for c in table["StorageDescriptor"]["Columns"])


def test_route_valid_json() -> None:
    payload = (
        b'{"source":"app_log","event_time":"2024-06-01T10:00:00+00:00","is_error":false}'
    )
    source, dt, hour, ok = _route(payload)
    assert (source, dt, hour, ok) == ("app_log", "2024-06-01", "10", True)


def test_route_unknown_and_malformed() -> None:
    source, _, _, ok = _route(b'{"event_time":"2024-06-01T10:00:00+00:00"}')
    assert source == "unknown" and ok is True
    source, _, _, ok = _route(b'{"source":"clickstream", BROKEN')
    assert source == "clickstream" and ok is False


def test_buffer_flush_thresholds() -> None:
    buf = Buffer("app_log", "2024-06-01", "10")
    for _ in range(4):
        buf.add(b"x")
    assert not buf.should_flush()
    assert buf.nbytes == 8
    buf.started -= 1000  # simulate age
    assert buf.should_flush()


def test_buffer_size_threshold(monkeypatch) -> None:
    monkeypatch.setattr("producers.consumer.FLUSH_BYTES", 10)
    buf = Buffer("app_log", "2024-06-01", "10")
    buf.add(b"y" * 9)
    assert buf.should_flush()


def test_datetime_helpers_are_utc() -> None:
    now = datetime.now(UTC)
    assert now.tzinfo is UTC
