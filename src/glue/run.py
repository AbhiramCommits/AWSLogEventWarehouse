"""Run the Glue/PySpark transform for one or both sources.

Stages the raw zone from S3 to a local directory (a laptop stand-in for the
S3A connector), runs :mod:`glue.transform_job`, uploads the curated Parquet
back to the curated bucket, and writes ``artifacts/transform_report.json``
with row counts, timings and the raw-vs-curated compression ratio.
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
import uuid
from pathlib import Path

from glue.transform_job import run_transform, spark_session
from logwarehouse.aws import client
from logwarehouse.config import Settings, get_settings


def _list_keys(s3, bucket: str, prefix: str) -> list[dict]:
    keys: list[dict] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(page.get("Contents", []))
    return keys


def stage_raw(s3, settings: Settings, source: str, dest: Path) -> tuple[int, int, int]:
    """Download a source's raw objects into a flat local directory.

    Returns ``(object_count, raw_bytes, distinct_prefixes)``.
    """
    prefix = f"{settings.raw_prefix}/source={source}/"
    keys = _list_keys(s3, settings.raw_bucket, prefix)
    dest.mkdir(parents=True, exist_ok=True)
    raw_bytes = 0
    prefixes: set[str] = set()
    for i, obj in enumerate(keys):
        key = obj["Key"]
        body = s3.get_object(Bucket=settings.raw_bucket, Key=key)["Body"].read()
        (dest / f"part-{i:05d}-{uuid.uuid4().hex[:6]}.json.gz").write_bytes(body)
        raw_bytes += len(body)
        prefixes.add("/".join(key.split("/")[:-1]))
    return len(keys), raw_bytes, len(prefixes)


def upload_curated(s3, settings: Settings, local_dir: Path, source: str) -> tuple[int, int]:
    """Upload local curated Parquet to ``curated/source=<source>/``."""
    files = sorted(p for p in local_dir.rglob("*") if p.is_file() and not p.name.startswith("_"))
    total = 0
    for path in files:
        rel = path.relative_to(local_dir).as_posix()
        key = f"{settings.curated_prefix}/source={source}/{rel}"
        body = path.read_bytes()
        s3.put_object(Bucket=settings.curated_bucket, Key=key, Body=body)
        total += len(body)
    return len(files), total


def _uncompressed_raw_bytes(settings: Settings) -> int | None:
    """Read the uncompressed raw byte count captured by the ingest step."""
    path = settings.artifacts_dir / "ingest_report.json"
    if not path.exists():
        return None
    return json.loads(path.read_text()).get("raw_bytes")


def transform_source(spark, settings: Settings, source: str, work: Path) -> dict:
    """Transform a single source end-to-end and return its report section."""
    s3 = client("s3", settings)
    raw_dir = work / f"raw_{source}"
    curated_dir = work / f"curated_{source}"
    quarantine_dir = work / f"quarantine_{source}"

    raw_objects, raw_gzip_bytes, raw_prefixes = stage_raw(s3, settings, source, raw_dir)
    report = run_transform(
        spark,
        str(raw_dir),
        str(curated_dir),
        str(quarantine_dir),
        source,
        run_date=time.strftime("%Y-%m-%d"),
    )
    file_count, curated_bytes = upload_curated(s3, settings, curated_dir, source)

    # Upload quarantined rows too, so they are inspectable in the curated bucket.
    if quarantine_dir.exists():
        q_files = [p for p in quarantine_dir.rglob("*") if p.is_file()]
        for path in q_files:
            key = f"quarantine/source={source}/{path.name}"
            s3.put_object(Bucket=settings.curated_bucket, Key=key, Body=path.read_bytes())

    report.update(
        {
            "raw_objects": raw_objects,
            "raw_partitions": raw_prefixes,
            "raw_gzip_bytes": raw_gzip_bytes,
            "curated_files": file_count,
            "curated_bytes": curated_bytes,
        }
    )
    return report


def run(sources: list[str], keep_workdir: bool = False) -> dict:
    """Run the transform for ``sources`` and write the aggregate report."""
    settings = get_settings()
    work = settings.artifacts_dir / "tmp" / "transform"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True, exist_ok=True)

    spark = spark_session()
    started = time.perf_counter()
    reports: dict[str, dict] = {}
    try:
        for source in sources:
            reports[source] = transform_source(spark, settings, source, work)
    finally:
        spark.stop()
    elapsed = time.perf_counter() - started

    totals = {
        "rows_in": sum(r["rows_in"] for r in reports.values()),
        "rows_out": sum(r["rows_out"] for r in reports.values()),
        "duplicates_removed": sum(r["duplicates_removed"] for r in reports.values()),
        "quarantined_rows": sum(r["quarantined_rows"] for r in reports.values()),
        "null_key_rows": sum(r["null_key_rows"] for r in reports.values()),
        "raw_gzip_bytes": sum(r["raw_gzip_bytes"] for r in reports.values()),
        "curated_bytes": sum(r["curated_bytes"] for r in reports.values()),
        "curated_files": sum(r["curated_files"] for r in reports.values()),
    }
    uncompressed = _uncompressed_raw_bytes(settings)
    totals["raw_uncompressed_bytes"] = uncompressed
    denominator = totals["curated_bytes"]
    if denominator:
        totals["compression_ratio_vs_uncompressed"] = (
            round(uncompressed / denominator, 2) if uncompressed else None
        )
        totals["compression_ratio_vs_gzip"] = round(totals["raw_gzip_bytes"] / denominator, 2)
    output = {
        "sources": reports,
        "totals": totals,
        "wall_seconds": round(elapsed, 3),
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    settings.artifacts_dir.mkdir(parents=True, exist_ok=True)
    (settings.artifacts_dir / "transform_report.json").write_text(json.dumps(output, indent=2))

    if not keep_workdir:
        shutil.rmtree(work, ignore_errors=True)
    return output


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Run the curated transform")
    parser.add_argument("--all", action="store_true", help="both sources")
    parser.add_argument("--source", choices=["app_log", "clickstream"], default=None)
    parser.add_argument("--keep-workdir", action="store_true")
    args = parser.parse_args(argv)

    sources = ["app_log", "clickstream"] if (args.all or not args.source) else [args.source]
    output = run(sources, keep_workdir=args.keep_workdir)
    print(json.dumps(output["totals"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
