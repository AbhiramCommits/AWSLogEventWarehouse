"""Centralised, environment-driven settings for the log-event warehouse.

Every value can be overridden with an environment variable so the exact same
code path runs against LocalStack (defaults) or real AWS (set the bucket/stream
names and leave ``AWS_ENDPOINT_URL`` pointing at ``https://<service>.<region>.amazonaws.com``
or unset it entirely).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


def _env_bool(name: str, default: bool) -> bool:
    return _env(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    """Immutable settings snapshot built from the process environment."""

    # AWS endpoint / auth
    endpoint_url: str = field(default_factory=lambda: _env("AWS_ENDPOINT_URL", "http://localhost:4566"))
    # LocalStack community does not implement the Glue + Firehose control
    # planes (they are "pro" features) and its kinesis-mock backend is far too
    # slow at volume, so a moto sidecar serves those three services locally
    # through the same boto3 API. Point these at real AWS (or set them empty to
    # reuse ``AWS_ENDPOINT_URL``) when running against a real account.
    kinesis_endpoint_url: str = field(
        default_factory=lambda: _env("KINESIS_ENDPOINT_URL", "http://localhost:4567")
    )
    glue_endpoint_url: str = field(default_factory=lambda: _env("GLUE_ENDPOINT_URL", "http://localhost:4567"))
    firehose_endpoint_url: str = field(
        default_factory=lambda: _env("FIREHOSE_ENDPOINT_URL", "http://localhost:4567")
    )
    region: str = field(default_factory=lambda: _env("AWS_REGION", "us-east-1"))
    access_key: str = field(default_factory=lambda: _env("AWS_ACCESS_KEY_ID", "test"))
    secret_key: str = field(default_factory=lambda: _env("AWS_SECRET_ACCESS_KEY", "test"))

    # Data lake resources
    raw_bucket: str = field(default_factory=lambda: _env("RAW_BUCKET", "logwarehouse-raw"))
    curated_bucket: str = field(default_factory=lambda: _env("CURATED_BUCKET", "logwarehouse-curated"))
    stream_name: str = field(default_factory=lambda: _env("STREAM_NAME", "log-events-stream"))
    firehose_name: str = field(default_factory=lambda: _env("FIREHOSE_NAME", "log-events-firehose"))
    glue_database: str = field(default_factory=lambda: _env("GLUE_DATABASE", "logwarehouse"))
    alarm_topic: str = field(default_factory=lambda: _env("ALARM_TOPIC", "pipeline-alarms"))
    log_group: str = field(default_factory=lambda: _env("LOG_GROUP", "/logwarehouse/pipeline"))

    # Key prefixes
    raw_prefix: str = field(default_factory=lambda: _env("RAW_PREFIX", "raw/events"))
    curated_prefix: str = field(default_factory=lambda: _env("CURATED_PREFIX", "curated"))

    # Local filesystem layout
    artifacts_dir: Path = field(
        default_factory=lambda: _repo_path(_env("ARTIFACTS_DIR", "artifacts"))
    )
    curated_local_path: Path = field(
        default_factory=lambda: _repo_path(_env("CURATED_LOCAL_PATH", "artifacts/curated"))
    )
    quarantine_local_path: Path = field(
        default_factory=lambda: _repo_path(_env("QUARANTINE_LOCAL_PATH", "artifacts/quarantine"))
    )
    duckdb_path: Path = field(
        default_factory=lambda: _repo_path(_env("DUCKDB_PATH", "artifacts/warehouse.duckdb"))
    )

    # Producer defaults (100k per source = 200,000 total). The local Kinesis
    # mocks degrade super-linearly with stream size (a 1M corpus would take
    # ~25 minutes), so the corpus is scaled to stay inside the ~10-minute
    # ingest budget. See docs/runbook.md.
    app_log_events: int = field(default_factory=lambda: _env_int("APP_LOG_EVENTS", 100_000))
    clickstream_events: int = field(default_factory=lambda: _env_int("CLICKSTREAM_EVENTS", 100_000))
    publish_batch: int = field(default_factory=lambda: _env_int("PUBLISH_BATCH", 500))
    random_seed: int = field(default_factory=lambda: _env_int("RANDOM_SEED", 42))

    # Alarm thresholds
    freshness_threshold_minutes: int = field(
        default_factory=lambda: _env_int("FRESHNESS_THRESHOLD_MINUTES", 120)
    )
    rowcount_floor: int = field(default_factory=lambda: _env_int("ROWCOUNT_FLOOR", 1))
    rowcount_drop_pct: float = field(default_factory=lambda: _env_float("ROWCOUNT_DROP_PCT", 60.0))
    error_rate_threshold: float = field(
        default_factory=lambda: _env_float("ERROR_RATE_THRESHOLD", 0.05)
    )
    quarantine_rate_threshold: float = field(
        default_factory=lambda: _env_float("QUARANTINE_RATE_THRESHOLD", 0.02)
    )

    def s3_uri(self, bucket: str, *parts: str) -> str:
        """Build an ``s3://bucket/key`` URI from path fragments."""
        key = "/".join(p.strip("/") for p in parts if p)
        return f"s3://{bucket}/{key}"

    @property
    def raw_uri(self) -> str:
        """S3 URI of the raw-event prefix."""
        return self.s3_uri(self.raw_bucket, self.raw_prefix)

    @property
    def curated_uri(self) -> str:
        """S3 URI of the curated prefix."""
        return self.s3_uri(self.curated_bucket, self.curated_prefix)


def _repo_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else _REPO_ROOT / path


_SETTINGS: Settings | None = None


def get_settings(reload: bool = False) -> Settings:
    """Return a cached :class:`Settings` instance (rebuilt when ``reload``)."""
    global _SETTINGS
    if _SETTINGS is None or reload:
        _SETTINGS = Settings()
    return _SETTINGS
