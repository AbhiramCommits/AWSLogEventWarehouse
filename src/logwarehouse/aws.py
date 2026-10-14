"""Thin boto3 helpers that point every client at the configured endpoint.

The only thing that changes between LocalStack and real AWS is
``settings.endpoint_url``: when it is ``None``/empty boto3 uses the standard
AWS endpoints and credential resolution. When an endpoint *is* configured
(LocalStack) the static test credentials from settings are injected so the
code runs even without a ``~/.aws/credentials`` file.
"""

from __future__ import annotations

from functools import cache
from typing import Any

import boto3
from botocore.client import Config

from logwarehouse.config import Settings, get_settings

# Per-service endpoint overrides (LocalStack community lacks Glue + Firehose
# and has an unusably slow Kinesis backend; the moto sidecar serves those).
_ENDPOINT_OVERRIDES = {
    "kinesis": "kinesis_endpoint_url",
    "glue": "glue_endpoint_url",
    "firehose": "firehose_endpoint_url",
}


@cache
def _client(
    service: str,
    endpoint: str | None,
    region: str,
    access_key: str | None,
    secret_key: str | None,
    max_attempts: int,
) -> Any:
    kwargs: dict[str, Any] = {
        "region_name": region,
        "config": Config(
            retries={"max_attempts": max_attempts, "mode": "standard"},
            s3={"addressing_style": "path"} if endpoint else {},
        ),
    }
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key
    return boto3.client(service, **kwargs)


def _endpoint_for(service: str, settings: Settings) -> str | None:
    """Resolve the endpoint for a service, honouring per-service overrides."""
    attr = _ENDPOINT_OVERRIDES.get(service)
    if attr:
        override = getattr(settings, attr, "") or ""
        if override:
            return override
    return settings.endpoint_url or None


def client(service: str, settings: Settings | None = None, max_attempts: int = 10) -> Any:
    """Return a cached boto3 client for ``service`` wired to the endpoint.

    Args:
        service: AWS service name, e.g. ``"s3"`` or ``"kinesis"``.
        settings: Optional settings override; defaults to :func:`get_settings`.
        max_attempts: Botocore retry attempts (lower it for fail-fast probes).

    Returns:
        A configured ``boto3`` client.
    """
    settings = settings or get_settings()
    endpoint = _endpoint_for(service, settings)
    if endpoint:
        return _client(service, endpoint, settings.region, settings.access_key,
                       settings.secret_key, max_attempts)
    return _client(service, None, settings.region, None, None, max_attempts)


@cache
def _resource(service: str, endpoint: str | None, region: str,
              access_key: str | None, secret_key: str | None) -> Any:
    kwargs: dict[str, Any] = {"region_name": region}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
        kwargs["config"] = Config(s3={"addressing_style": "path"})
    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key
    return boto3.resource(service, **kwargs)


def resource(service: str, settings: Settings | None = None) -> Any:
    """Return a cached boto3 *resource* for ``service`` (e.g. ``"s3"``)."""
    settings = settings or get_settings()
    endpoint = _endpoint_for(service, settings)
    if endpoint:
        return _resource(service, endpoint, settings.region, settings.access_key,
                         settings.secret_key)
    return _resource(service, None, settings.region, None, None)


def reset_clients() -> None:
    """Clear the client cache (used by tests and by ``--reload`` runs)."""
    _client.cache_clear()
    _resource.cache_clear()
