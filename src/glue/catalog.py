"""Register curated tables and partitions in the Glue Data Catalog.

Uses the same boto3 ``create_database`` / ``create_table`` /
``batch_create_partition`` calls the AWS Glue crawler would produce, then
discovers partitions by listing the curated S3 prefix (a crawler-equivalent).
"""

from __future__ import annotations

import contextlib
import json
import time
from typing import Any

from botocore.exceptions import ClientError

from logwarehouse.aws import client
from logwarehouse.config import Settings, get_settings

# Explicit Glue/Hive column definitions matching the curated Parquet output.
CURATED_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "app_log": [
        ("event_id", "string"),
        ("event_time", "string"),
        ("service", "string"),
        ("host", "string"),
        ("level", "string"),
        ("status_code", "int"),
        ("latency_ms", "int"),
        ("route", "string"),
        ("user_id", "string"),
        ("trace_id", "string"),
        ("message", "string"),
        ("source", "string"),
        ("ingest_time", "string"),
        ("event_ts", "timestamp"),
        ("hour", "string"),
        ("ingest_lag_seconds", "bigint"),
        ("is_null_key", "boolean"),
        ("status_class", "string"),
        ("is_error", "boolean"),
    ],
    "clickstream": [
        ("event_id", "string"),
        ("event_time", "string"),
        ("session_id", "string"),
        ("user_id", "string"),
        ("page", "string"),
        ("referrer", "string"),
        ("device", "string"),
        ("country", "string"),
        ("action", "string"),
        ("dwell_ms", "int"),
        ("experiment_variant", "string"),
        ("source", "string"),
        ("ingest_time", "string"),
        ("event_ts", "timestamp"),
        ("hour", "string"),
        ("ingest_lag_seconds", "bigint"),
        ("is_null_key", "boolean"),
        ("is_error", "boolean"),
    ],
}

PARQUET_SERDE = (
    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
)
PARQUET_INPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
PARQUET_OUTPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"


def table_name(source: str) -> str:
    """Return the catalog table name for a source."""
    return f"curated_{source}"


def curated_location(settings: Settings, source: str) -> str:
    """Return the ``s3://`` location of a source's curated table."""
    return settings.s3_uri(settings.curated_bucket, settings.curated_prefix, f"source={source}")


def ensure_database(settings: Settings) -> str:
    """Create the Glue database if missing and return its name."""
    glue = client("glue", settings)
    try:
        glue.create_database(DatabaseInput={"Name": settings.glue_database})
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "AlreadyExistsException":
            raise
    return settings.glue_database


def _table_input(settings: Settings, source: str) -> dict[str, Any]:
    columns = [{"Name": n, "Type": t} for n, t in CURATED_SCHEMAS[source]]
    return {
        "Name": table_name(source),
        "Description": f"Curated events for source={source}",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet", "source": source},
        "PartitionKeys": [{"Name": "dt", "Type": "string"}],
        "StorageDescriptor": {
            "Columns": columns,
            "Location": curated_location(settings, source),
            "InputFormat": PARQUET_INPUT,
            "OutputFormat": PARQUET_OUTPUT,
            "Compressed": True,
            "SerdeInfo": {
                "SerializationLibrary": PARQUET_SERDE,
                "Parameters": {"path": curated_location(settings, source)},
            },
        },
    }


def ensure_table(settings: Settings, source: str) -> str:
    """Create or update a curated table definition."""
    glue = client("glue", settings)
    table = table_name(source)
    table_input = _table_input(settings, source)
    try:
        glue.get_table(DatabaseName=settings.glue_database, Name=table)
        glue.update_table(DatabaseName=settings.glue_database, TableInput=table_input)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        glue.create_table(DatabaseName=settings.glue_database, TableInput=table_input)
    return table


def discover_partitions(settings: Settings, source: str) -> list[str]:
    """List distinct ``dt`` partition values from the curated S3 prefix."""
    s3 = client("s3", settings)
    prefix = f"{settings.curated_prefix}/source={source}/"
    dts: set[str] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=settings.curated_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            for part in obj["Key"].split("/"):
                if part.startswith("dt=") and len(part) > 3:
                    dts.add(part[3:])
    return sorted(dts)


def register_partitions(settings: Settings, source: str, dts: list[str]) -> int:
    """Batch-register partitions, ignoring ones already present."""
    if not dts:
        return 0
    glue = client("glue", settings)
    table = table_name(source)
    location = curated_location(settings, source)
    columns = [{"Name": n, "Type": t} for n, t in CURATED_SCHEMAS[source]]
    storage = {
        "Columns": columns,
        "InputFormat": PARQUET_INPUT,
        "OutputFormat": PARQUET_OUTPUT,
        "Compressed": True,
        "SerdeInfo": {"SerializationLibrary": PARQUET_SERDE},
    }
    existing = set()
    with contextlib.suppress(ClientError):
        existing = {
            p["Values"][0]
            for p in glue.get_partitions(DatabaseName=settings.glue_database, TableName=table)[
                "Partitions"
            ]
        }
    new = [dt for dt in dts if dt not in existing]
    if not new:
        return 0
    glue.batch_create_partition(
        DatabaseName=settings.glue_database,
        TableName=table,
        PartitionInputList=[
            {"Values": [dt], "StorageDescriptor": {**storage, "Location": f"{location}/dt={dt}"}}
            for dt in new
        ],
    )
    return len(new)


def catalog_summary(settings: Settings) -> dict[str, Any]:
    """Return a summary of tables and partitions currently in the catalog."""
    glue = client("glue", settings)
    summary: dict[str, Any] = {"database": settings.glue_database, "tables": {}}
    for table in glue.get_tables(DatabaseName=settings.glue_database)["TableList"]:
        name = table["Name"]
        parts = glue.get_partitions(DatabaseName=settings.glue_database, TableName=name)[
            "Partitions"
        ]
        summary["tables"][name] = {
            "location": table["StorageDescriptor"]["Location"],
            "columns": [c["Name"] for c in table["StorageDescriptor"]["Columns"]],
            "partition_keys": [p["Name"] for p in table.get("PartitionKeys", [])],
            "partitions": sorted(p["Values"][0] for p in parts),
        }
    return summary


def run(sources: list[str] | None = None) -> dict[str, Any]:
    """Create database/tables, discover + register partitions, print schemas."""
    settings = get_settings()
    ensure_database(settings)
    for source in sources or ["app_log", "clickstream"]:
        ensure_table(settings, source)
        dts = discover_partitions(settings, source)
        added = register_partitions(settings, source, dts)
        print(f"{table_name(source)}: registered {added} new partitions ({len(dts)} total)")
    summary = catalog_summary(settings)
    (settings.artifacts_dir / "catalog_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    return summary


def main() -> int:
    """CLI entry point."""
    started = time.perf_counter()
    run()
    print(f"catalog registered in {time.perf_counter() - started:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
