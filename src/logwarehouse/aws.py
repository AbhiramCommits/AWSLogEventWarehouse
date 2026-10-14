"""Thin boto3 helpers that point every client at the configured endpoint.

The only thing that changes between LocalStack and real AWS is
``settings.endpoint_url``: when it is ``None``/empty boto3 uses the standard
AWS endpoints and credential resolution.
"""

from __future__ import annotations

from functools import cache
from typing import Any

import boto3
from botocore.client import Config

from logwarehouse.config import Settings, get_settings


@cache
def _client(service: str, endpoint: str | None, region: str) -> Any:
    kwargs: dict[str, Any] = {
        "region_name": region,
        "config": Config(
            retries={"max_attempts": 10, "mode": "standard"},
            s3={"addressing_style": "path"} if endpoint else {},
        ),
    }
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client(service, **kwargs)


def client(service: str, settings: Settings | None = None) -> Any:
    """Return a cached boto3 client for ``service`` wired to the endpoint.

    Args:
        service: AWS service name, e.g. ``"s3"`` or ``"kinesis"``.
        settings: Optional settings override; defaults to :func:`get_settings`.

    Returns:
        A configured ``boto3`` client.
    """
    settings = settings or get_settings()
    endpoint = _endpoint_for(service, settings)
    return _client(service, endpoint, settings.region)


_ENDPOINT_OVERRIDES = {
    "glue": "glue_endpoint_url",
    "firehose": "firehose_endpoint_url",
}


def _endpoint_for(service: str, settings: Settings) -> str | None:
    """Resolve the endpoint for a service, honouring per-service overrides."""
    attr = _ENDPOINT_OVERRIDES.get(service)
    if attr:
        override = getattr(settings, attr, "") or ""
        if override:
            return override
    return settings.endpoint_url or None


@cache
def _resource(service: str, endpoint: str | None, region: str) -> Any:
    kwargs: dict[str, Any] = {"region_name": region}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
        kwargs["config"] = Config(s3={"addressing_style": "path"})
    return boto3.resource(service, **kwargs)


def resource(service: str, settings: Settings | None = None) -> Any:
    """Return a cached boto3 *resource* for ``service`` (e.g. ``"s3"``)."""
    settings = settings or get_settings()
    endpoint = settings.endpoint_url or None
    return _resource(service, endpoint, settings.region)


def reset_clients() -> None:
    """Clear the client cache (used by tests and by ``--reload`` runs)."""
    _client.cache_clear()
    _resource.cache_clear()
