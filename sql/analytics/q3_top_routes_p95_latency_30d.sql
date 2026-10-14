-- q3: top routes by p95 latency over the last 30 days.
--
-- Computes p50/p95/p99 latency, average latency and error rate per route,
-- ranks routes by p95 and compares each route's p95 with the global average
-- via a window function. Joins dim_service for the owning service.
--
-- Tuning hooks: dt predicate push-down (30-day partition pruning), columnar
-- projection of only latency/error columns, and the fact's physical sort on
-- (service_sk, event_ts) which keeps per-service route scans clustered.

WITH route_stats AS (
    SELECT
        f.route                                   AS route,
        s.service                                 AS service,
        count(*)                                  AS requests,
        avg(f.latency_ms)                         AS avg_latency_ms,
        quantile_cont(f.latency_ms, 0.50)         AS p50_latency_ms,
        quantile_cont(f.latency_ms, 0.95)         AS p95_latency_ms,
        quantile_cont(f.latency_ms, 0.99)         AS p99_latency_ms,
        sum(CASE WHEN f.is_error THEN 1 ELSE 0 END)::DOUBLE / NULLIF(count(*), 0) AS error_rate
    FROM fact_event_activity f
    JOIN dim_service s USING (service_sk)
    WHERE f.source = 'app_log'
      AND f.dt >= current_date - INTERVAL 30 DAY
      AND f.route IS NOT NULL
    GROUP BY f.route, s.service
)
SELECT
    route,
    service,
    requests,
    round(avg_latency_ms, 2) AS avg_latency_ms,
    p50_latency_ms,
    p95_latency_ms,
    p99_latency_ms,
    round(error_rate, 4) AS error_rate,
    rank() OVER (ORDER BY p95_latency_ms DESC) AS p95_rank,
    round(p95_latency_ms - avg(p95_latency_ms) OVER (), 2) AS p95_vs_global_avg
FROM route_stats
ORDER BY p95_latency_ms DESC
LIMIT 20;
