#!/usr/bin/env python3
"""Capture screenshots of the running Streamlit dashboard.

Starts ``streamlit run src/dashboard/app.py`` headlessly, waits for it to be
ready, then uses Playwright/Chromium to capture each tab into ``docs/img/``.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "img"
PORT = 8501
TABS = [
    ("Event Volume", "dashboard_volume.png"),
    ("Latency", "dashboard_latency.png"),
    ("Reliability", "dashboard_reliability.png"),
    ("Freshness & Alarms", "dashboard_freshness.png"),
    ("Query Performance", "dashboard_performance.png"),
]


def _wait_for_port(port: int, timeout: int = 90) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as sock:
            sock.settimeout(1)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


def main() -> int:
    """Start Streamlit, capture every tab, stop Streamlit."""
    OUT.mkdir(parents=True, exist_ok=True)
    streamlit = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "src/dashboard/app.py",
         "--server.port", str(PORT), "--server.headless", "true",
         "--browser.gatherUsageStats", "false"],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        if not _wait_for_port(PORT):
            print("streamlit did not start", file=sys.stderr)
            return 1
        time.sleep(8)  # let the first render finish
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page(viewport={"width": 1440, "height": 900})
            page.goto(f"http://127.0.0.1:{PORT}", wait_until="networkidle", timeout=60_000)
            page.wait_for_selector("text=Event volume", timeout=30_000)
            time.sleep(3)
            for label, filename in TABS:
                try:
                    page.get_by_role("tab", name=label, exact=False).click(timeout=10_000)
                    time.sleep(3.5)
                except Exception as exc:  # noqa: BLE001
                    print(f"could not click {label}: {exc}", file=sys.stderr)
                page.screenshot(path=str(OUT / filename), full_page=True)
                print(f"captured {filename}")
            browser.close()
    finally:
        streamlit.terminate()
        streamlit.wait(timeout=15)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
