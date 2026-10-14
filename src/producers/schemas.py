"""Event contracts for the two log producers.

Two sources publish JSON events into Kinesis:

* ``app_log`` — application request logs.
* ``clickstream`` — user interaction / funnel events.

The raw JSON also carries a ``source`` discriminator so the delivery worker
and the Glue transform can tell the two streams apart after they are merged
into a single Kinesis stream. See :meth:`validate_event`.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

APP_LOG = "app_log"
CLICKSTREAM = "clickstream"
SOURCES = (APP_LOG, CLICKSTREAM)

LEVELS = ("INFO", "WARN", "ERROR")
ACTIONS = ("view", "click", "add_to_cart", "purchase")
DEVICES = ("desktop", "mobile", "tablet")
VARIANTS = ("control", "A", "B")

APP_LOG_FIELDS = (
    "event_id",
    "event_time",
    "service",
    "host",
    "level",
    "status_code",
    "latency_ms",
    "route",
    "user_id",
    "trace_id",
    "message",
)

CLICKSTREAM_FIELDS = (
    "event_id",
    "event_time",
    "session_id",
    "user_id",
    "page",
    "referrer",
    "device",
    "country",
    "action",
    "dwell_ms",
    "experiment_variant",
)


@dataclass(slots=True)
class AppLogEvent:
    """Application log event contract."""

    event_id: str
    event_time: str
    service: str
    host: str
    level: str
    status_code: int
    latency_ms: int
    route: str
    user_id: str | None
    trace_id: str
    message: str
    source: str = APP_LOG

    def to_dict(self) -> dict[str, Any]:
        """Return the event as a plain dict (ready for JSON encoding)."""
        return asdict(self)


@dataclass(slots=True)
class ClickstreamEvent:
    """Clickstream / funnel event contract."""

    event_id: str
    event_time: str
    session_id: str | None
    user_id: str | None
    page: str
    referrer: str
    device: str
    country: str
    action: str
    dwell_ms: int
    experiment_variant: str
    source: str = CLICKSTREAM

    def to_dict(self) -> dict[str, Any]:
        """Return the event as a plain dict (ready for JSON encoding)."""
        return asdict(self)


def key_field(source: str) -> str:
    """Return the primary (null-able) key field for a source.

    ``app_log`` is keyed by ``user_id`` and ``clickstream`` by ``session_id``;
    the injected null-key defects blank these columns.
    """
    return "user_id" if source == APP_LOG else "session_id"


def partition_key(source: str, event: dict[str, Any]) -> str:
    """Return the Kinesis partition key for an event.

    ``app_log`` partitions by ``service`` and ``clickstream`` by
    ``session_id``; a null key falls back to the literal ``"null"``.
    """
    field = "service" if source == APP_LOG else "session_id"
    value = event.get(field)
    return str(value) if value not in (None, "") else "null"


def fields_for(source: str) -> tuple[str, ...]:
    """Return the tuple of contract field names for a source."""
    return APP_LOG_FIELDS if source == APP_LOG else CLICKSTREAM_FIELDS


def validate_event(source: str, event: dict[str, Any]) -> list[str]:
    """Validate an event against its contract.

    Args:
        source: ``"app_log"`` or ``"clickstream"``.
        event: Parsed event dictionary.

    Returns:
        A list of human-readable errors; empty when the event is valid.
        Null keys are *allowed* (they are an injected defect, flagged elsewhere).
    """
    errors: list[str] = []
    for field in fields_for(source):
        if field not in event:
            errors.append(f"missing field: {field}")

    if source == APP_LOG:
        if event.get("level") not in LEVELS:
            errors.append(f"invalid level: {event.get('level')!r}")
        code = event.get("status_code")
        if not isinstance(code, int) or not (100 <= code <= 599):
            errors.append(f"invalid status_code: {code!r}")
        if not isinstance(event.get("latency_ms"), int):
            errors.append("latency_ms must be an int")
    else:
        if event.get("action") not in ACTIONS:
            errors.append(f"invalid action: {event.get('action')!r}")
        if event.get("device") not in DEVICES:
            errors.append(f"invalid device: {event.get('device')!r}")
        if event.get("experiment_variant") not in VARIANTS:
            errors.append(f"invalid experiment_variant: {event.get('experiment_variant')!r}")
        if not isinstance(event.get("dwell_ms"), int):
            errors.append("dwell_ms must be an int")
    return errors
