#!/usr/bin/env python3
"""Wait until the LocalStack health endpoint reports ready."""

from __future__ import annotations

import sys
import time

import requests

from logwarehouse.config import get_settings


def wait(timeout: int = 120) -> bool:
    """Poll the LocalStack health endpoint until ready or ``timeout`` seconds."""
    settings = get_settings()
    base = settings.endpoint_url or "http://localhost:4566"
    url = f"{base.rstrip('/')}/_localstack/health"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(url, timeout=3)
            if resp.status_code == 200:
                print(f"LocalStack ready at {base}")
                return True
        except requests.RequestException:
            pass
        print("waiting for LocalStack ...")
        time.sleep(3)
    print("LocalStack did not become ready in time", file=sys.stderr)
    return False


if __name__ == "__main__":
    raise SystemExit(0 if wait() else 1)
