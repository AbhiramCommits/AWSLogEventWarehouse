"""Deterministic, seeded synthetic log-event generator.

Produces realistic traffic with deliberately injected defects so the rest of
the pipeline has something to clean up:

======================  =======  =========================================
Defect                  Rate     How it is represented in the raw payload
======================  =======  =========================================
duplicate event_id      ~2%      an exact copy of a recent event
malformed / unparseable ~1%      JSON truncated + corrupted (source intact)
null key                ~0.5%    ``user_id`` / ``session_id`` set to null
late arrival            ~3%      ``ingest_time`` 5-60 min after ``event_time``
======================  =======  =========================================

Volume follows a diurnal curve, and one service (``checkout``) has an elevated
error-rate incident window covering the most recent two hours.
"""

from __future__ import annotations

import json
import math
import random
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from producers.schemas import (
    ACTIONS,
    APP_LOG,
    CLICKSTREAM,
    DEVICES,
    VARIANTS,
    partition_key,
)

DUP_RATE = 0.02
MALFORMED_RATE = 0.01
NULL_KEY_RATE = 0.005
LATE_RATE = 0.03

SERVICES = ("auth", "search", "catalog", "checkout", "payments", "recommendations")
INCIDENT_SERVICE = "checkout"
INCIDENT_HOURS = 2.0
BASELINE_ERROR_RATE = 0.02
INCIDENT_ERROR_RATE = 0.30

ROUTES = {
    "auth": ("/api/v1/login", "/api/v1/logout", "/api/v1/token/refresh"),
    "search": ("/api/v1/search", "/api/v1/search/suggest"),
    "catalog": ("/api/v1/products", "/api/v1/products/{id}"),
    "checkout": ("/api/v1/cart", "/api/v1/checkout", "/api/v1/payment"),
    "payments": ("/api/v1/payments/authorize", "/api/v1/payments/capture"),
    "recommendations": ("/api/v1/recommendations", "/api/v1/trending"),
}
PAGES = ("/home", "/search", "/product/1", "/product/2", "/cart", "/checkout", "/account")
REFERRERS = ("direct", "google", "facebook", "newsletter", "partner")
COUNTRIES = ("US", "GB", "DE", "IN", "BR", "CA", "FR", "AU", "JP", "MX")
MESSAGES = {
    "INFO": "request completed",
    "WARN": "slow request",
    "ERROR": "request failed",
}


@dataclass(slots=True)
class GeneratedRecord:
    """A single generated Kinesis record plus the defects it carries."""

    source: str
    partition_key: str
    data: bytes
    event: dict | None
    event_time: datetime
    is_duplicate: bool = False
    is_malformed: bool = False
    is_null_key: bool = False
    is_late: bool = False

    @property
    def is_valid(self) -> bool:
        """True when the record is parseable JSON (i.e. not malformed)."""
        return not self.is_malformed


def diurnal_weight(hour: int) -> float:
    """Return a relative traffic weight for an hour of day (0-23)."""
    morning = math.exp(-((hour - 9) ** 2) / (2 * 3.0**2))
    evening = math.exp(-((hour - 19) ** 2) / (2 * 3.5**2))
    return 0.15 + morning + 1.2 * evening


def _sample_event_time(rng: random.Random, end_time: datetime, span_hours: float) -> datetime:
    """Sample an event time in the window, shaped by the diurnal curve."""
    span_minutes = int(span_hours * 60)
    peak = max(diurnal_weight(h) for h in range(24))
    for _ in range(64):
        minutes_ago = rng.uniform(0, span_minutes)
        candidate = end_time - timedelta(minutes=minutes_ago)
        if rng.random() <= diurnal_weight(candidate.hour) / peak:
            return candidate
    return end_time - timedelta(minutes=span_minutes / 2)


def _new_id(rng: random.Random) -> str:
    return f"{rng.getrandbits(128):032x}"


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat()


def _status_code(rng: random.Random, error: bool) -> int:
    if error:
        return rng.choice((500, 502, 503, 504))
    return rng.choices(
        (200, 201, 204, 400, 404, 429),
        weights=(70, 8, 5, 6, 8, 3),
        k=1,
    )[0]


def _build_app_log(rng: random.Random, ts: datetime, incident: bool) -> dict:
    service = INCIDENT_SERVICE if incident else rng.choice(SERVICES)
    error = rng.random() < (INCIDENT_ERROR_RATE if incident else BASELINE_ERROR_RATE)
    if rng.random() < 0.10:
        level = "WARN"
    else:
        level = "ERROR" if error else "INFO"
    status = _status_code(rng, error)
    latency = int(rng.lognormvariate(4.0, 0.6)) + (800 if error else 0)
    route = rng.choice(ROUTES[service]).replace("{id}", str(rng.randint(1, 500)))
    host = f"ip-10-0-{rng.randint(0, 255)}-{rng.randint(1, 254)}"
    return {
        "source": APP_LOG,
        "event_id": _new_id(rng),
        "event_time": _iso(ts),
        "service": service,
        "host": host,
        "level": level,
        "status_code": status,
        "latency_ms": latency,
        "route": route,
        "user_id": f"u{rng.randint(1, 50_000):06d}",
        "trace_id": _new_id(rng),
        "message": f"{route} -> {status} in {latency}ms ({MESSAGES[level]})",
    }


def _build_clickstream(rng: random.Random, ts: datetime) -> dict:
    action = rng.choices(ACTIONS, weights=(60, 25, 10, 5), k=1)[0]
    page = rng.choice(PAGES)
    return {
        "source": CLICKSTREAM,
        "event_id": _new_id(rng),
        "event_time": _iso(ts),
        "session_id": f"s{rng.getrandbits(48):012x}",
        "user_id": f"u{rng.randint(1, 50_000):06d}",
        "page": page,
        "referrer": rng.choice(REFERRERS),
        "device": rng.choices(DEVICES, weights=(55, 38, 7), k=1)[0],
        "country": rng.choice(COUNTRIES),
        "action": action,
        "dwell_ms": int(rng.lognormvariate(8.0, 1.0)),
        "experiment_variant": rng.choices(VARIANTS, weights=(40, 30, 30), k=1)[0],
    }


def _to_bytes(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def _corrupt(payload: bytes) -> bytes:
    """Return a genuinely unparseable payload that still exposes ``source``."""
    text = payload.decode("utf-8")
    keep = text[: max(32, len(text) // 3)]
    return (keep + ' ",," "broken\\x00":').encode("utf-8")


def iter_records(
    source: str,
    count: int,
    seed: int = 42,
    end_time: datetime | None = None,
    span_hours: float = 24.0,
) -> Iterator[GeneratedRecord]:
    """Yield ``count`` generated records for ``source``.

    The stream is identical for a given ``(source, count, seed, end_time)``.
    """
    if source not in (APP_LOG, CLICKSTREAM):
        raise ValueError(f"unknown source: {source!r}")
    rng = random.Random(f"{seed}:{source}")
    end_time = end_time or datetime.now(UTC)
    incident_start = end_time - timedelta(hours=INCIDENT_HOURS)
    reservoir: deque[dict] = deque(maxlen=2000)

    def _generate():
        for _ in range(count):
            ts = _sample_event_time(rng, end_time, span_hours)
            incident = source == APP_LOG and ts >= incident_start

            roll = rng.random()
            if roll < DUP_RATE and reservoir:
                event = dict(rng.choice(reservoir))
                event_time = datetime.fromisoformat(event["event_time"])
                yield GeneratedRecord(
                    source, partition_key(source, event), _to_bytes(event),
                    event, event_time, is_duplicate=True,
                )
                continue
            if roll < DUP_RATE + MALFORMED_RATE:
                event = (
                    _build_app_log(rng, ts, incident)
                    if source == APP_LOG
                    else _build_clickstream(rng, ts)
                )
                data = _corrupt(_to_bytes(event))
                yield GeneratedRecord(
                    source, "malformed", data, None, ts, is_malformed=True,
                )
                continue

            event = (
                _build_app_log(rng, ts, incident)
                if source == APP_LOG
                else _build_clickstream(rng, ts)
            )
            is_null = rng.random() < NULL_KEY_RATE
            is_late = rng.random() < LATE_RATE
            if is_null:
                event["user_id"] = None
                if source == CLICKSTREAM:
                    event["session_id"] = None
            gap = rng.uniform(5 * 60, 60 * 60) if is_late else rng.uniform(0, 90)
            event["ingest_time"] = _iso(ts + timedelta(seconds=gap))
            reservoir.append(dict(event))
            yield GeneratedRecord(
                source,
                partition_key(source, event),
                _to_bytes(event),
                event,
                ts,
                is_null_key=is_null,
                is_late=is_late,
            )

    return _generate()


def defect_counts(records: list[GeneratedRecord]) -> dict[str, int]:
    """Aggregate injected-defect counts for a materialised record list."""
    return {
        "total": len(records),
        "duplicates": sum(r.is_duplicate for r in records),
        "malformed": sum(r.is_malformed for r in records),
        "null_key": sum(r.is_null_key for r in records),
        "late": sum(r.is_late for r in records),
    }
