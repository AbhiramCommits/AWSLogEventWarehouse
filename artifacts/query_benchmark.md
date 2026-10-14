# Query tuning benchmark

5 measured runs per query (one warm-up discarded).

| Query | Baseline median (s) | Tuned median (s) | Baseline p95 (s) | Tuned p95 (s) | Speedup (median) | Rows scanned (base → tuned) | Bytes scanned (base → tuned) | Results match |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| q1_hourly_error_rate_by_service | 0.116468 | 0.000406 | 0.121705 | 0.000506 | 286.87x | 193,143 → 140 | 32,273,230 → 2,674 | True |
| q2_funnel_conversion_by_variant | 0.114193 | 0.003166 | 0.118276 | 0.003598 | 36.07x | 193,143 → 29,720 | 32,273,230 → 1,669,415 | True |
| q3_top_routes_p95_latency | 0.111968 | 0.001197 | 0.118999 | 0.003174 | 93.54x | 193,143 → 29,720 | 32,273,230 → 1,669,415 | True |

## Techniques credited

- **q1** — pre-aggregated hourly rollup Parquet replaces a full fact scan and CSV vs columnar compression.
- **q2** — partitioned Snappy Parquet with `dt` predicate push-down and columnar projection (no uncompressed CSV scan).
- **q3** — partitioned Snappy Parquet, `dt` pruning, physical sort on `(service, event_ts)` and narrow projection (only route/latency/error columns).
