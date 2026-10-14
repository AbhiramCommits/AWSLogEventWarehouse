-- q2: funnel conversion by experiment variant.
--
-- Distinct sessions are counted at each funnel stage (view -> click ->
-- add_to_cart -> purchase), stage-to-stage conversion rates are derived and
-- variants are ranked by overall conversion. Joins the clickstream fact to
-- dim_page for stage attribution and uses window functions for the ranking.
--
-- Tuning hooks: partitioned Snappy Parquet scan with dt pruning, a narrow
-- projection over the fact, and rewrite of repeated scans into a single pass.

WITH stages AS (
    SELECT
        f.experiment_variant AS variant,
        count(DISTINCT CASE WHEN f.action = 'view'        THEN f.event_id END) AS viewers,
        count(DISTINCT CASE WHEN f.action = 'click'       THEN f.event_id END) AS clickers,
        count(DISTINCT CASE WHEN f.action = 'add_to_cart' THEN f.event_id END) AS carters,
        count(DISTINCT CASE WHEN f.action = 'purchase'    THEN f.event_id END) AS purchasers,
        count(DISTINCT f.page_sk)                                              AS pages_touched
    FROM fact_event_activity f
    WHERE f.source = 'clickstream'
      AND f.dt >= current_date - INTERVAL 30 DAY
    GROUP BY f.experiment_variant
)
SELECT
    variant,
    viewers,
    clickers,
    carters,
    purchasers,
    pages_touched,
    clickers::DOUBLE / NULLIF(viewers, 0)    AS view_to_click,
    carters::DOUBLE / NULLIF(clickers, 0)    AS click_to_cart,
    purchasers::DOUBLE / NULLIF(carters, 0)  AS cart_to_purchase,
    purchasers::DOUBLE / NULLIF(viewers, 0)  AS overall_conversion,
    rank() OVER (ORDER BY purchasers::DOUBLE / NULLIF(viewers, 0) DESC) AS variant_rank
FROM stages
ORDER BY variant;
