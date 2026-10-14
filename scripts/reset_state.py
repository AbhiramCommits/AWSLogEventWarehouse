#!/usr/bin/env python3
"""Reset local pipeline state so runs are reproducible.

Empties the raw + curated S3 buckets, deletes and recreates the Kinesis
stream (records cannot be deleted in place), removes generated artifacts and
optionally deletes the Firehose delivery stream. Intended for local/dev use.
"""

from __future__ import annotations

import argparse
import time

from botocore.exceptions import ClientError

from logwarehouse.aws import client
from logwarehouse.config import get_settings


def empty_bucket(bucket: str) -> int:
    """Delete every object in ``bucket`` and return the number removed."""
    s3 = client("s3")
    removed = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
            removed += len(keys)
    return removed


def reset_stream(stream: str, shards: int = 2) -> None:
    """Delete and recreate a Kinesis stream, waiting for ACTIVE."""
    kinesis = client("kinesis")
    try:
        kinesis.delete_stream(StreamName=stream)
        for _ in range(60):
            time.sleep(2)
            try:
                kinesis.describe_stream(StreamName=stream)
            except ClientError:
                break
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    kinesis.create_stream(StreamName=stream, ShardCount=shards)
    waiter = kinesis.get_waiter("stream_exists")
    waiter.wait(StreamName=stream)
    print(f"recreated kinesis stream {stream}")


def delete_firehose(name: str) -> None:
    """Best-effort delete of a Firehose delivery stream."""
    firehose = client("firehose")
    try:
        firehose.delete_delivery_stream(DeliveryStreamName=name, AllowForceDelete=True)
        print(f"deleted firehose stream {name}")
    except ClientError as exc:
        print(f"firehose delete skipped: {exc.response['Error']['Code']}")


def reset_artifacts() -> None:
    """Remove generated report artifacts (keeps .gitkeep)."""
    settings = get_settings()
    for name in (
        "ingest_manifest.json",
        "ingest_report.json",
        "transform_report.json",
        "query_benchmark.json",
        "query_benchmark.md",
        "pipeline_run.json",
        "alarm_evidence.json",
        "metrics_snapshot.json",
    ):
        path = settings.artifacts_dir / name
        if path.exists():
            path.unlink()


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Reset local pipeline state")
    parser.add_argument("--keep-firehose", action="store_true")
    args = parser.parse_args()
    settings = get_settings()

    print(f"emptied raw bucket: {empty_bucket(settings.raw_bucket)} objects")
    print(f"emptied curated bucket: {empty_bucket(settings.curated_bucket)} objects")
    reset_stream(settings.stream_name)
    if not args.keep_firehose:
        delete_firehose(settings.firehose_name)
    reset_artifacts()
    print("reset complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
