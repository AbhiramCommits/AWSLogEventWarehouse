#!/usr/bin/env python3
"""Provision the LocalStack "pro" resources (Glue DB + Firehose) via boto3.

Terraform owns every resource that LocalStack community supports. The Glue
Data Catalog and the Firehose control plane are LocalStack *pro* features, so
locally they are created here through the same boto3 APIs used against real
AWS (they are defined in Terraform too, gated behind ``use_localstack``).
"""

from __future__ import annotations

import sys

from botocore.exceptions import ClientError

from logwarehouse.aws import client
from logwarehouse.config import get_settings


def ensure_glue_database() -> str:
    """Create the Glue catalog database if it does not already exist."""
    settings = get_settings()
    glue = client("glue", settings)
    try:
        glue.create_database(DatabaseInput={"Name": settings.glue_database})
        print(f"created glue database {settings.glue_database}")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "AlreadyExistsException":
            raise
        print(f"glue database {settings.glue_database} already exists")
    return settings.glue_database


def ensure_firehose() -> str:
    """Create the Firehose delivery stream (Kinesis -> S3 raw) if missing."""
    settings = get_settings()
    firehose = client("firehose", settings)
    stream_arn = f"arn:aws:kinesis:{settings.region}:000000000000:stream/{settings.stream_name}"
    role_arn = f"arn:aws:iam::000000000000:role/{settings.firehose_name}-role"
    bucket_arn = f"arn:aws:s3:::{settings.raw_bucket}"
    prefix = "raw/events/source=app_log/dt=!{timestamp:yyyy-MM-dd}/hour=!{timestamp:HH}/"

    try:
        firehose.create_delivery_stream(
            DeliveryStreamName=settings.firehose_name,
            DeliveryStreamType="KinesisStreamAsSource",
            KinesisStreamSourceConfiguration={
                "KinesisStreamARN": stream_arn,
                "RoleARN": role_arn,
            },
            ExtendedS3DestinationConfiguration={
                "RoleARN": role_arn,
                "BucketARN": bucket_arn,
                "Prefix": prefix,
                "CompressionFormat": "GZIP",
                "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 60},
            },
        )
        print(f"created firehose delivery stream {settings.firehose_name}")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code not in {"ResourceInUseException", "ValidationException"}:
            raise
        print(f"firehose delivery stream {settings.firehose_name} already exists ({code})")
    return settings.firehose_name


def list_streams() -> list[str]:
    """Return the names of Kinesis streams currently visible at the endpoint."""
    kinesis = client("kinesis")
    return kinesis.list_streams()["StreamNames"]


def main() -> int:
    """Bootstrap local pro resources and print the Kinesis streams."""
    ensure_glue_database()
    ensure_firehose()
    streams = list_streams()
    print(f"kinesis streams: {streams}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
