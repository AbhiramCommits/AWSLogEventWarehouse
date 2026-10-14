# Troubleshooting runbook

Operational playbook for the AWSLogEventWarehouse pipeline. Each entry has
**symptom → diagnose → fix**.

## Local stack notes (read first)

- Most services run on **LocalStack** (`http://localhost:4566`).
- **Kinesis, Glue and Firehose** are served by the **moto** sidecar
  (`http://localhost:4567`). LocalStack community does not implement the
  Glue/Firehose control planes and its `kinesis-mock` backend degrades
  super-linearly (a 200k-event publish drops from ~9k to ~300 rec/s), which
  would push a 1M-event corpus past 25 minutes. The code always talks to the
  genuine AWS APIs via boto3/terraform; only the endpoint changes. On real AWS
  unset `KINESIS_ENDPOINT_URL`, `GLUE_ENDPOINT_URL` and `FIREHOSE_ENDPOINT_URL`
  (they fall back to `AWS_ENDPOINT_URL`, then to real AWS).
- **boto3/botocore are pinned to 1.34.162.** Newer botocore breaks the
  CloudWatch query protocol against both LocalStack and moto
  (`Missing Action in request for query-protocol service`).

## 1. Stuck shard iterator

**Symptom** — `make produce` hangs or `GetRecords` returns
`ExpiredIteratorException`; the consumer never reaches `expected_total`.

**Diagnose**
```bash
# how many records are actually in the stream?
python -c "from logwarehouse.aws import client; k=client('kinesis'); \
print(k.describe_stream_summary(StreamName='log-events-stream'))"
docker logs --tail 50 logwarehouse-moto
```

**Fix** — Kinesis iterators expire after 5 minutes idle. The consumer starts
from `TRIM_HORIZON` on every run, so re-run `make produce`; if the stream was
reset mid-read, reset and re-run cleanly:
```bash
python scripts/reset_state.py     # recreates the stream
make produce
```
If the shard count changed while a run was in flight, `reset_state.py` waits for
the old stream to delete before recreating it.

## 2. Firehose not delivering

**Symptom** — records are in Kinesis but nothing appears in
`s3://logwarehouse-raw/raw/events/`; `ingest_report.json` shows
`"delivery_path": "worker"`.

**Diagnose**
```bash
python -c "import json;print(json.load(open('artifacts/ingest_report.json'))['delivery_reason'])"
```
`probe_firehose()` in `src/producers/consumer.py` attempts a real
`PutRecordBatch` and reports why it fell back.

**Fix** — this is expected locally: LocalStack's Firehose accepts records but
does not deliver them, so the Firehose-equivalent worker takes over and writes
the partitioned gzip objects itself. On real AWS the probe finds delivered
objects and returns `"delivery_path": "firehose"`. Nothing to fix locally; if
you *want* the native path locally, run against real AWS.

## 3. Small-file explosion

**Symptom** — `transform_report.json` shows a large `curated_files` count with
tiny average file size; DuckDB scans many small Parquet files.

**Diagnose**
```bash
python -c "import json;t=json.load(open('artifacts/transform_report.json'))['totals']; \
print(t['curated_files'], t['curated_bytes']//max(t['curated_files'],1), 'avg bytes')"
```

**Fix** — the Glue job controls file sizing via `repartition(parts, "dt")`
(`output_partitions`, default 4). Increase `--output-partitions` for larger
corpora, or reduce it if files are too small. `glue/run.py` clears the curated
prefix before each upload, so stale files from previous runs cannot accumulate
(which would otherwise inflate counts and double-count rows).

## 4. Schema drift in raw JSON

**Symptom** — curated row counts drop sharply; `quarantined_rows` spikes;
`quarantine/source=...` objects fill up.

**Diagnose**
```bash
python - <<'PY'
import duckdb, json
con=duckdb.connect()
print(con.execute("SELECT * FROM read_json_auto('s3://...') LIMIT 0"))
PY
# or inspect quarantine payloads
aws --endpoint-url http://localhost:4566 s3 ls s3://logwarehouse-curated/quarantine/ --recursive
```

**Fix** — the raw read uses an explicit schema with `PERMISSIVE` mode and
`columnNameOfCorruptRecord`, so unknown columns are ignored and unparseable
rows are quarantined rather than failing the job. When a producer deliberately
adds a field, add it to `APP_LOG_SCHEMA`/`CLICKSTREAM_SCHEMA` in
`src/glue/transform_job.py` (and `sql/ddl_athena.sql`). Re-run `make transform`
after reprocessing the quarantined rows.

## 5. Partition not registered in Glue Catalog

**Symptom** — `make catalog` prints `0 new partitions`, or Athena/QuickSight
returns zero rows despite Parquet existing in S3.

**Diagnose**
```bash
python -c "from glue.catalog import catalog_summary; from logwarehouse.config import get_settings; \
import json; print(json.dumps(catalog_summary(get_settings()), indent=2))"
```

**Fix** — `catalog.register_partitions` discovers `dt=` values straight from
the curated S3 prefix (crawler-equivalent) and is idempotent. If a partition is
missing, the curated upload may have failed (check `make transform` logs) or the
S3 prefix layout changed. On real AWS, register a Glue crawler over the curated
prefix as a backstop. Note: the Glue catalog is served by moto locally; a moto
restart clears it, so re-run `make catalog`.

## 6. Alarm false positives

**Symptom** — `logwarehouse-*` alarms flap to `ALARM` on healthy data, or the
`alarms --check` CI gate fails unexpectedly.

**Diagnose**
```bash
python -m monitoring.alarms --check --json
python -c "import json;print(json.dumps(json.load(open('artifacts/metrics_snapshot.json')),indent=2))"
```

**Fix** — thresholds are configurable (`FRESHNESS_THRESHOLD_MINUTES`,
`ROWCOUNT_FLOOR`, `ROWCOUNT_DROP_PCT`, `ERROR_RATE_THRESHOLD`,
`QUARANTINE_RATE_THRESHOLD`). Common causes: (a) the diurnal trough genuinely
drops volume — raise `ROWCOUNT_DROP_PCT` or widen the evaluation window;
(b) the tiny CI corpus trips the drop alarm — the `--small` pipeline run
applies relaxed thresholds automatically; (c) an incident override file is still
present — clear it with `python -m monitoring.simulate_incident --restore`.
Alarms use `TreatMissingData=breaching` for the row-count signals (a stalled
producer must alert) and are wired to the `pipeline-alarms` SNS topic.

## Incident drill

Reproduce a stalled-producer + error-spike window and confirm the alarms fire:
```bash
make monitor                 # healthy: --check passes
make incident                # publishes degraded metrics; alarms -> ALARM
python -m monitoring.alarms --check   # exits 1 (breach)
make restore                 # clear the incident
```
Evidence (fired alarm names + states) is written to
`artifacts/alarm_evidence.json`.
