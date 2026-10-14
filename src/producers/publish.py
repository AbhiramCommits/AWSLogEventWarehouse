"""Publish generated events into a Kinesis Data Stream.

Usage::

    python -m producers.publish --source app_log --events 250000 --batch 500

Batches ``PutRecords`` calls, retries throttled/partial failures with
exponential backoff, and reports throughput. A manifest is written to
``artifacts/ingest_manifest.json`` so the delivery worker knows how many
records to drain from the stream.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

from botocore.exceptions import ClientError

from logwarehouse.aws import client
from logwarehouse.config import get_settings
from producers.generate import GeneratedRecord, iter_records
from producers.schemas import APP_LOG, SOURCES

log = logging.getLogger("producers.publish")

MAX_KINESIS_BATCH = 500
MAX_KINESIS_BYTES = 5 * 1024 * 1024
RETRYABLE = {
    "ProvisionedThroughputExceededException",
    "LimitExceededException",
    "ThrottlingException",
    "InternalFailure",
    "ServiceUnavailable",
}


def _flush(kinesis, stream: str, records: list[GeneratedRecord]) -> tuple[int, int]:
    """Send one batch, retrying partial/throttled failures.

    Returns ``(records_sent, attempts)``.
    """
    payload = [
        {"Data": r.data, "PartitionKey": r.partition_key or "null"} for r in records
    ]
    attempts = 0
    pending = payload
    sent = 0
    while pending:
        attempts += 1
        try:
            resp = kinesis.put_records(StreamName=stream, Records=pending)
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code not in RETRYABLE or attempts > 8:
                raise
            time.sleep(min(2 ** attempts * 0.01, 1.0))
            continue
        failed = resp.get("FailedRecordCount", 0)
        if failed:
            retry: list[dict] = []
            for item, result in zip(pending, resp["Records"], strict=True):
                if result.get("ErrorCode"):
                    retry.append(item)
            sent += len(pending) - failed
            pending = retry
            time.sleep(min(2 ** attempts * 0.01, 1.0))
        else:
            sent += len(pending)
            pending = []
    return sent, attempts


def publish(
    source: str,
    events: int,
    batch: int = MAX_KINESIS_BATCH,
    seed: int | None = None,
) -> dict:
    """Generate and publish ``events`` records for ``source`` to Kinesis.

    Args:
        source: ``"app_log"`` or ``"clickstream"``.
        events: Number of records to publish.
        batch: Target ``PutRecords`` batch size (capped at 500).
        seed: RNG seed; defaults to ``settings.random_seed``.

    Returns:
        A summary dict with counts, bytes, seconds and records/sec.
    """
    settings = get_settings()
    kinesis = client("kinesis", settings)
    seed = settings.random_seed if seed is None else seed
    batch = max(1, min(batch, MAX_KINESIS_BATCH))
    stream = settings.stream_name

    total = 0
    total_bytes = 0
    attempts = 0
    buffer: list[GeneratedRecord] = []
    buffer_bytes = 0
    start = time.perf_counter()

    def _drain_buffer() -> None:
        nonlocal total, attempts, buffer, buffer_bytes
        if not buffer:
            return
        sent, tries = _flush(kinesis, stream, buffer)
        total += sent
        attempts += tries
        buffer = []
        buffer_bytes = 0

    for record in iter_records(source, events, seed=seed):
        buffer.append(record)
        buffer_bytes += len(record.data) + 40
        total_bytes += len(record.data)
        if len(buffer) >= batch or buffer_bytes >= MAX_KINESIS_BYTES:
            _drain_buffer()
            if total % 50_000 == 0:
                log.info("published %s records for %s", total, source)
    _drain_buffer()

    elapsed = time.perf_counter() - start
    summary = {
        "source": source,
        "events": total,
        "bytes": total_bytes,
        "seconds": round(elapsed, 3),
        "records_per_sec": round(total / elapsed, 1) if elapsed else 0.0,
        "put_records_calls": attempts,
        "stream": stream,
    }
    log.info(
        "published %s %s events in %.2fs (%.0f rec/s)",
        total, source, elapsed, summary["records_per_sec"],
    )
    return summary


def _write_manifest(summary: dict) -> Path:
    settings = get_settings()
    settings.artifacts_dir.mkdir(parents=True, exist_ok=True)
    path = settings.artifacts_dir / "ingest_manifest.json"
    data = json.loads(path.read_text()) if path.exists() else {"sources": {}}
    data["sources"][summary["source"]] = summary
    data["total_events"] = sum(s["events"] for s in data["sources"].values())
    path.write_text(json.dumps(data, indent=2))
    return path


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Publish log events to Kinesis")
    parser.add_argument("--source", choices=SOURCES, default=APP_LOG)
    parser.add_argument("--events", type=int, default=None, help="record count")
    parser.add_argument("--batch", type=int, default=MAX_KINESIS_BATCH)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = get_settings()
    events = args.events
    if events is None:
        events = (
            settings.app_log_events if args.source == APP_LOG else settings.clickstream_events
        )
    summary = publish(args.source, events, batch=args.batch, seed=args.seed)
    _write_manifest(summary)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
