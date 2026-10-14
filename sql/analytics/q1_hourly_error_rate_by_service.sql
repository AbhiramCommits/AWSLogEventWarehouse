-- q1: hourly error rate by service, with a trailing 24-hour moving average and
-- a within-hour error-rank. Aggregates the full app_log fact with a dimension
-- join and window functions.
--
-- Tuning hooks exercised here: dt predicate push-down (partition pruning),
-- narrow projection (only the columns the aggregation needs) and the fact's
-- physical sort on (service_sk, event_ts).

WITH hourly AS (
    SELECT
        f.dt                                          AS event_date,
        f.hour                                        AS event_hour,
        s.service                                     AS service,
        s.service_group                               AS service_group,
        count(*)                                      AS requests,
        sum(CASE WHEN f.is_error THEN 1 ELSE 0 END)   AS errors
    FROM fact_event_activity f
    JOIN dim_service s USING (service_sk)
    WHERE f.source = 'app_log'
      AND f.dt >= current_date - INTERVAL 30 DAY
    GROUP BY f.dt, f.hour, s.service, s.service_group
)
SELECT
    event_date,
    event_hour,
    service,
    service_group,
    requests,
    errors,
    errors::DOUBLE / NULLIF(requests, 0) AS error_rate,
    avg(errors::DOUBLE / NULLIF(requests, 0)) OVER (
        PARTITION BY service
        ORDER BY event_date, event_hour
        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) AS error_rate_24h_ma,
    rank() OVER (
        PARTITION BY event_date, event_hour
        ORDER BY errors::DOUBLE / NULLIF(requests, 0) DESC
    ) AS error_rank
FROM hourly
ORDER BY event_date DESC, event_hour DESC, error_rate DESC;
