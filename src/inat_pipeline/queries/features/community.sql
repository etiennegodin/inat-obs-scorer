--point-in-time trailing community RG rate

-- At score anchor date T, trailing community resolution rate
CREATE OR REPLACE TABLE features.community AS
WITH daily_rg AS (
    SELECT
        created_at::DATE AS rg_date,
        COUNT(*) AS new_rg_count,
        COUNT(*) FILTER (WHERE id_created_at IS NOT NULL) AS resolved_count,
        COUNT(DISTINCT(user_id)) AS observer_count,
        COUNT(DISTINCT(identifier_ct)) AS identifier_count
    FROM staged.rg_events
    -- point-in-time: first_rg_at is when it actually became RG
    GROUP BY created_at::DATE

),

window_totals AS (
    -- rolling 90d sum using window function — one pass
    SELECT
        rg_date,
        SUM(new_rg_count) OVER (
            ORDER BY rg_date
            RANGE BETWEEN INTERVAL '90 days' PRECEDING AND INTERVAL '1 day' PRECEDING
        ) AS window_obs_count,
        SUM(resolved_count) OVER (
            ORDER BY rg_date
            RANGE BETWEEN INTERVAL '90 days' PRECEDING AND INTERVAL '1 day' PRECEDING
        ) AS window_rg_count,
        SUM(observer_count) OVER (
            ORDER BY rg_date
            RANGE BETWEEN INTERVAL '90 days' PRECEDING AND INTERVAL '1 day' PRECEDING
        ) AS window_observer_count,
        SUM(identifier_count) OVER (
            ORDER BY rg_date
            RANGE BETWEEN INTERVAL '90 days' PRECEDING AND INTERVAL '1 day' PRECEDING
        ) AS window_identifier_count
    FROM daily_rg
)

SELECT
    o.id AS observation_id,
    o.created_at,
    wt.window_rg_count * 1.0
    / NULLIF(wt.window_obs_count, 0) AS trailing_community_rg_rate_90d,
    wt.window_obs_count AS community_window_count,
    wt.window_observer_count / wt.window_obs_count AS observer_ratio,
    wt.window_identifier_count / wt.window_obs_count AS identifier_ratio
FROM staged.observations o
LEFT JOIN window_totals wt
    ON wt.rg_date = (o.created_at + INTERVAL '7 days')::DATE
WHERE wt.window_obs_count > 1
