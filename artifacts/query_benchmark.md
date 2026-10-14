# Query tuning benchmark

5 measured runs per query (one warm-up discarded).

| Query | Baseline median (s) | Tuned median (s) | Baseline p95 (s) | Tuned p95 (s) | Speedup (median) | Rows scanned (base → tuned) | Bytes scanned (base → tuned) | Results match |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| q1_hourly_error_rate_by_service | 0.173888 | 0.000694 | 0.198214 | 0.000803 | 250.56x | 193,128 → 140 | 32,267,011 → 2,671 | True |
| q2_funnel_conversion_by_variant | 0.173625 | 0.006535 | 0.180103 | 0.006989 | 26.57x | 193,128 → 35,125 | 32,267,011 → 1,968,529 | True |
| q3_top_routes_p95_latency | 0.156456 | 0.001528 | 0.168384 | 0.002086 | 102.39x | 193,128 → 35,125 | 32,267,011 → 1,968,529 | True |

## Techniques credited

- **q1** — pre-aggregated hourly rollup Parquet replaces a full fact scan and CSV vs columnar compression.
- **q2** — partitioned Snappy Parquet with `dt` predicate push-down and columnar projection (no uncompressed CSV scan).
- **q3** — partitioned Snappy Parquet, `dt` pruning, physical sort on `(service, event_ts)` and narrow projection (only route/latency/error columns).
