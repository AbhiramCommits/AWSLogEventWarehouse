"""Glue 4.0-compatible PySpark transformation job.

Reads raw newline-delimited JSON from the raw zone, cleans it and writes
curated Snappy Parquet partitioned by ``dt``::

    raw/events/source=app_log/dt=.../*.json.gz
        -> curated/source=app_log/dt=.../*.parquet

The job is written so it runs unchanged on AWS Glue 4.0: the only
Glue-specific coupling (``awsglue`` imports and the GlueContext bootstrap) is
isolated in :func:`build_glue_context`, which is a no-op off Glue.

Stages
------
1. read with an explicit schema in ``PERMISSIVE`` mode; unparseable rows are
   captured in ``_corrupt_record`` and routed to a quarantine path;
2. normalise timestamps to UTC, derive ``dt``/``hour`` and ``ingest_lag_seconds``;
3. de-duplicate on ``event_id`` keeping the earliest ``ingest_time``;
4. drop and count null-key rows, cast types and enrich
   (``status_class``, ``is_error``, device/country cleanup);
5. write partitioned Snappy Parquet with controlled file sizing;
6. emit a JSON run report.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

APP_LOG_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_time", StringType()),
        StructField("service", StringType()),
        StructField("host", StringType()),
        StructField("level", StringType()),
        StructField("status_code", IntegerType()),
        StructField("latency_ms", IntegerType()),
        StructField("route", StringType()),
        StructField("user_id", StringType()),
        StructField("trace_id", StringType()),
        StructField("message", StringType()),
        StructField("source", StringType()),
        StructField("ingest_time", StringType()),
        StructField("_corrupt_record", StringType()),
    ]
)

CLICKSTREAM_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_time", StringType()),
        StructField("session_id", StringType()),
        StructField("user_id", StringType()),
        StructField("page", StringType()),
        StructField("referrer", StringType()),
        StructField("device", StringType()),
        StructField("country", StringType()),
        StructField("action", StringType()),
        StructField("dwell_ms", IntegerType()),
        StructField("experiment_variant", StringType()),
        StructField("source", StringType()),
        StructField("ingest_time", StringType()),
        StructField("_corrupt_record", StringType()),
    ]
)

KEY_FIELD = {"app_log": "user_id", "clickstream": "session_id"}


def _to_epoch(value: str | None) -> int | None:
    """Parse an ISO-8601 string to epoch seconds (UTC), or ``None``."""
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(value).timestamp())
    except (ValueError, TypeError):
        return None


_EPOCH_UDF = None


def _epoch_udf():
    """Return a cached Spark UDF wrapping :func:`_to_epoch`."""
    global _EPOCH_UDF
    if _EPOCH_UDF is None:
        _EPOCH_UDF = F.udf(_to_epoch, LongType())
    return _EPOCH_UDF


def build_glue_context(args: dict[str, Any] | None = None) -> Any:
    """Return a ``GlueContext`` when running on AWS Glue, else ``None``.

    All ``awsglue`` imports live here so the module imports cleanly on a
    laptop where the Glue libraries are absent.
    """
    try:  # pragma: no cover - only available inside AWS Glue
        from awsglue.context import GlueContext

        spark = SparkSession.builder.getOrCreate()
        return GlueContext(spark.sparkContext)
    except Exception:
        return None


def spark_session(app_name: str = "logwarehouse-curated-transform") -> SparkSession:
    """Create (or fetch) a local Spark session configured for the job."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def raw_schema(source: str) -> StructType:
    """Return the explicit raw schema for a source."""
    return APP_LOG_SCHEMA if source == "app_log" else CLICKSTREAM_SCHEMA


def _read_raw(spark: SparkSession, raw_path: str, source: str) -> DataFrame:
    return (
        spark.read.schema(raw_schema(source))
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(raw_path)
    )


def _enrich(df: DataFrame, source: str) -> DataFrame:
    """Add derived columns shared by both sources."""
    epoch = _epoch_udf()
    event_ts = F.to_timestamp(F.from_unixtime(epoch("event_time")))
    ingest_epoch = epoch("ingest_time")
    event_epoch = epoch("event_time")
    out = (
        df.withColumn("event_ts", event_ts)
        .withColumn("dt", F.date_format("event_ts", "yyyy-MM-dd"))
        .withColumn("hour", F.date_format("event_ts", "HH"))
        .withColumn("ingest_lag_seconds", (ingest_epoch - event_epoch).cast("long"))
        .withColumn("is_null_key", F.col(KEY_FIELD[source]).isNull())
    )
    if source == "app_log":
        out = (
            out.withColumn("status_class", F.concat(F.substring("status_code", 1, 1), F.lit("xx")))
            .withColumn(
                "is_error",
                (F.col("status_class") == "5xx") | (F.col("level") == "ERROR"),
            )
            .withColumn("level", F.upper(F.trim("level")))
            .withColumn("route", F.lower(F.trim("route")))
        )
    else:
        out = (
            out.withColumn("device", F.lower(F.trim("device")))
            .withColumn("country", F.upper(F.trim("country")))
            .withColumn("is_error", F.lit(False))
        )
    return out


def _dedupe(df: DataFrame) -> tuple[DataFrame, int]:
    """Drop duplicate ``event_id``s, keeping the earliest ``ingest_time``."""
    window = Window.partitionBy("event_id").orderBy(F.col("ingest_lag_seconds").asc_nulls_last())
    ranked = df.withColumn("_rn", F.row_number().over(window))
    deduped = ranked.filter(F.col("_rn") == 1).drop("_rn")
    return deduped, df.count() - deduped.count()


def run_transform(
    spark: SparkSession,
    raw_path: str,
    curated_path: str,
    quarantine_path: str,
    source: str,
    run_date: str | None = None,
    output_partitions: int = 4,
) -> dict[str, Any]:
    """Run the full transform for one source and return a run report.

    Args:
        spark: Active :class:`SparkSession`.
        raw_path: Directory of raw ``.json.gz`` objects (local or ``s3://``).
        curated_path: Output Parquet prefix.
        quarantine_path: Output prefix for unparseable rows.
        source: ``"app_log"`` or ``"clickstream"``.
        run_date: Optional logical run date recorded in the report.
        output_partitions: Target number of output files (file-size control).

    Returns:
        A report dict with row counts, defect counts, lag percentiles, file
        count, average file size and wall-clock seconds.
    """
    started = time.perf_counter()
    raw = _read_raw(spark, raw_path, source).persist()
    rows_in = raw.count()

    corrupt = raw.filter(F.col("_corrupt_record").isNotNull())
    quarantined = corrupt.count()
    valid = raw.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    enriched = _enrich(valid, source)
    null_key_rows = enriched.filter(F.col("is_null_key")).count()
    non_null = enriched.filter(~F.col("is_null_key"))

    deduped, duplicates_removed = _dedupe(non_null)
    rows_out = deduped.count()

    lag = deduped.select("ingest_lag_seconds").where(F.col("ingest_lag_seconds").isNotNull())
    quantiles = lag.approxQuantile("ingest_lag_seconds", [0.5, 0.95, 0.99], 0.001)
    p50, p95, p99 = (quantiles + [0.0, 0.0, 0.0])[:3] if quantiles else (0.0, 0.0, 0.0)

    n_dt = deduped.select("dt").distinct().count() or 1
    parts = max(n_dt, min(output_partitions, n_dt * output_partitions))
    deduped.repartition(parts, "dt").write.mode("overwrite").partitionBy("dt").parquet(curated_path)

    if quarantined:
        (
            corrupt.select(F.col("_corrupt_record").alias("raw_payload"))
            .withColumn("reason", F.lit("json_parse_error"))
            .withColumn("source", F.lit(source))
            .withColumn("quarantined_at", F.current_timestamp())
            .coalesce(1)
            .write.mode("overwrite")
            .json(quarantine_path)
        )

    raw.unpersist()
    elapsed = time.perf_counter() - started
    return {
        "source": source,
        "run_date": run_date,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "duplicates_removed": duplicates_removed,
        "quarantined_rows": quarantined,
        "null_key_rows": null_key_rows,
        "ingest_lag_p50": float(p50),
        "ingest_lag_p95": float(p95),
        "ingest_lag_p99": float(p99),
        "output_partitions": parts,
        "distinct_dt": n_dt,
        "wall_seconds": round(elapsed, 3),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse Glue job arguments (``--raw-path`` etc.)."""
    parser = argparse.ArgumentParser(description="Glue-compatible curated transform")
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--curated-path", required=True)
    parser.add_argument("--quarantine-path", required=True)
    parser.add_argument("--source", required=True, choices=["app_log", "clickstream"])
    parser.add_argument("--run-date", default=None)
    parser.add_argument("--report-path", default=None)
    parser.add_argument("--output-partitions", type=int, default=4)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point (also valid as a Glue ``--job-language python`` script)."""
    args = parse_args(argv)
    build_glue_context(vars(args))
    spark = spark_session()
    report = run_transform(
        spark,
        args.raw_path,
        args.curated_path,
        args.quarantine_path,
        args.source,
        run_date=args.run_date,
        output_partitions=args.output_partitions,
    )
    print(json.dumps(report, indent=2))
    if args.report_path:
        Path(args.report_path).write_text(json.dumps(report, indent=2))
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
