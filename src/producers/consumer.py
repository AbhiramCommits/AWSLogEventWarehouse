"""Firehose-equivalent delivery worker.

The delivery layer lands events into the S3 raw zone, partitioned by
``source``/``dt``/``hour``. Two paths exist:

* **firehose** — the native Firehose delivery stream consumes the Kinesis
  stream and writes to S3 itself. This is exercised first with a real
  ``PutRecordBatch`` call.
* **worker** — this module reads the Kinesis shards directly, buffers by
  record count / size / time, gzips newline-delimited JSON and writes
  partitioned objects to S3.

LocalStack's community Firehose accepts records but does not actually deliver
them, so the worker path is normally selected; the chosen path is logged and
recorded in ``artifacts/ingest_report.json``.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import re
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from botocore.exceptions import ClientError

from logwarehouse.aws import client
from logwarehouse.config import get_settings

log = logging.getLogger("producers.consumer")

FLUSH_COUNT = 5_000
FLUSH_BYTES = 5 * 1024 * 1024
FLUSH_SECONDS = 15.0
MAX_RECORDS_PER_GET = 10_000
SOURCE_RE = re.compile(rb'"source"\s*:\s*"([a-z_]+)"')
SENTINEL = b'{"source":"probe","probe":true}'


@dataclass
class Buffer:
    """Accumulates raw payloads for one (source, dt, hour) partition."""

    source: str
    dt: str
    hour: str
    lines: list[bytes] = field(default_factory=list)
    nbytes: int = 0
    started: float = field(default_factory=time.perf_counter)

    def add(self, payload: bytes) -> None:
        self.lines.append(payload)
        self.nbytes += len(payload) + 1

    def should_flush(self) -> bool:
        """True when count, size or age thresholds are exceeded."""
        return (
            len(self.lines) >= FLUSH_COUNT
            or self.nbytes >= FLUSH_BYTES
            or (time.perf_counter() - self.started) >= FLUSH_SECONDS
        )


def _s3_list(s3, bucket: str, prefix: str) -> set[str]:
    keys: set[str] = set()
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            keys.add(obj["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys


def probe_firehose(wait_seconds: float = 8.0) -> tuple[str, str]:
    """Attempt native Firehose delivery and report which path to use.

    Returns ``(path, reason)`` where ``path`` is ``"firehose"`` or ``"worker"``.
    """
    settings = get_settings()
    firehose = client("firehose", settings, max_attempts=1)
    s3 = client("s3", settings)
    prefix = f"{settings.raw_prefix}/"
    try:
        before = _s3_list(s3, settings.raw_bucket, prefix)
        firehose.put_record_batch(
            DeliveryStreamName=settings.firehose_name,
            Records=[{"Data": SENTINEL}],
        )
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        return "worker", f"firehose put_record_batch failed ({code})"

    deadline = time.perf_counter() + wait_seconds
    while time.perf_counter() < deadline:
        time.sleep(2.0)
        if _s3_list(s3, settings.raw_bucket, prefix) - before:
            return "firehose", "native firehose delivered to S3"
    return "worker", "firehose accepted records but delivered nothing to S3"


def _get_shards(kinesis, stream: str) -> list[str]:
    return [s["ShardId"] for s in kinesis.list_shards(StreamName=stream)["Shards"]]


def _iterators(kinesis, stream: str) -> dict[str, str]:
    iters: dict[str, str] = {}
    for shard in _get_shards(kinesis, stream):
        iters[shard] = kinesis.get_shard_iterator(
            StreamName=stream, ShardId=shard, ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]
    return iters


def _route(payload: bytes) -> tuple[str, str, str, bool]:
    """Return ``(source, dt, hour, parsed_ok)`` for a raw payload."""
    source = "unknown"
    event_time: datetime | None = None
    parsed_ok = True
    try:
        event = json.loads(payload)
        if not isinstance(event, dict):
            raise ValueError("payload is not a JSON object")
        source = event.get("source") or "unknown"
        if event.get("event_time"):
            event_time = datetime.fromisoformat(event["event_time"])
    except (ValueError, TypeError):
        parsed_ok = False
        match = SOURCE_RE.search(payload)
        if match:
            source = match.group(1).decode("utf-8")
    if event_time is None:
        event_time = datetime.now(UTC)
    event_time = event_time.astimezone(UTC)
    return source, event_time.strftime("%Y-%m-%d"), event_time.strftime("%H"), parsed_ok


class Delivery:
    """Reads Kinesis and writes buffered, gzipped objects to the S3 raw zone."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self.kinesis = client("kinesis", self.settings)
        self.s3 = client("s3", self.settings)
        self.buffers: dict[tuple[str, str, str], Buffer] = {}
        self.partitions: set[tuple[str, str, str]] = set()
        self.objects = 0
        self.s3_bytes = 0
        self.raw_bytes = 0
        self.records = 0
        self.malformed = 0
        self.per_source: Counter[str] = Counter()

    def _flush(self, key: tuple[str, str, str]) -> None:
        buf = self.buffers.pop(key)
        if not buf.lines:
            return
        body = gzip.compress(b"\n".join(buf.lines) + b"\n")
        source, dt, hour = key
        key_name = (
            f"{self.settings.raw_prefix}/source={source}/dt={dt}/hour={hour}/"
            f"part-{uuid.uuid4().hex[:12]}.json.gz"
        )
        self.s3.put_object(
            Bucket=self.settings.raw_bucket, Key=key_name, Body=body,
            ContentType="application/gzip",
        )
        self.objects += 1
        self.s3_bytes += len(body)
        self.partitions.add(key)

    def add(self, payload: bytes) -> None:
        """Buffer one raw payload, flushing when thresholds are exceeded."""
        source, dt, hour, parsed_ok = _route(payload)
        key = (source, dt, hour)
        buf = self.buffers.get(key)
        if buf is None:
            buf = Buffer(*key)
            self.buffers[key] = buf
        buf.add(payload)
        self.raw_bytes += len(payload)
        self.records += 1
        self.per_source[source] += 1
        if not parsed_ok:
            self.malformed += 1
        if buf.should_flush():
            self._flush(key)

    def flush_all(self) -> None:
        """Flush every open buffer."""
        for key in list(self.buffers):
            self._flush(key)

    def drain(self, expected_total: int | None = None, max_idle_rounds: int = 15) -> None:
        """Read the Kinesis stream until drained or ``expected_total`` reached."""
        iters = _iterators(self.kinesis, self.settings.stream_name)
        idle = 0
        while iters and idle < max_idle_rounds:
            progressed = False
            for shard in list(iters):
                iterator = iters[shard]
                if iterator is None:
                    iters.pop(shard, None)
                    continue
                resp = self.kinesis.get_records(ShardIterator=iterator, Limit=MAX_RECORDS_PER_GET)
                for rec in resp["Records"]:
                    self.add(rec["Data"])
                    progressed = True
                iters[shard] = resp.get("NextShardIterator")
                if expected_total and self.records >= expected_total:
                    break
            if expected_total and self.records >= expected_total:
                break
            idle = 0 if progressed else idle + 1
            if not progressed:
                time.sleep(0.5)
        self.flush_all()


def _expected_total() -> int | None:
    settings = get_settings()
    manifest = settings.artifacts_dir / "ingest_manifest.json"
    if not manifest.exists():
        return None
    data = json.loads(manifest.read_text())
    return data.get("total_events")


def deliver_all() -> dict:
    """Run delivery and return the ingest report dict."""
    settings = get_settings()
    path, reason = probe_firehose()
    log.info("delivery path: %s (%s)", path, reason)

    expected = _expected_total()
    start = time.perf_counter()
    delivery = Delivery()
    delivery.drain(expected_total=expected)
    elapsed = time.perf_counter() - start

    report = {
        "delivery_path": path,
        "delivery_reason": reason,
        "total_events": delivery.records,
        "raw_bytes": delivery.raw_bytes,
        "s3_objects": delivery.objects,
        "s3_gzip_bytes": delivery.s3_bytes,
        "partitions": len(delivery.partitions),
        "per_source": dict(delivery.per_source),
        "malformed_records": delivery.malformed,
        "seconds": round(elapsed, 3),
        "records_per_sec": round(delivery.records / elapsed, 1) if elapsed else 0.0,
        "expected_total": expected,
    }
    settings.artifacts_dir.mkdir(parents=True, exist_ok=True)
    (settings.artifacts_dir / "ingest_report.json").write_text(json.dumps(report, indent=2))
    log.info(
        "delivered %s events, %s objects, %s partitions in %.2fs",
        delivery.records, delivery.objects, len(delivery.partitions), elapsed,
    )
    return report


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for the delivery worker."""
    parser = argparse.ArgumentParser(description="Firehose-equivalent S3 delivery worker")
    parser.add_argument("--deliver-all", action="store_true", help="drain the whole stream")
    parser.add_argument("--print-partitions", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.deliver_all:
        report = deliver_all()
        print(json.dumps(report, indent=2))
        return 0

    if args.print_partitions:
        s3 = client("s3")
        settings = get_settings()
        keys = sorted(_s3_list(s3, settings.raw_bucket, settings.raw_prefix))
        print(json.dumps(keys, indent=2))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
