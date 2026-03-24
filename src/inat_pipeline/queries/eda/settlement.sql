-- Run once per interval, union the results
CREATE OR REPLACE TABLE eda.settlement AS

WITH intervals AS (
    SELECT unnest([7, 14, 30, 60, 90, 180]) AS days
),
settled_final AS (
    -- Ground truth: what's RG with unlimited window
    SELECT observation_id, is_rg AS eventually_rg
    FROM research_grade_windowed(INTERVAL '999 years')
),
settled_at_window AS (
    -- What's RG at each checkpoint
    SELECT 1  AS days, observation_id, is_rg FROM research_grade_windowed(INTERVAL '1 days')
    UNION ALL
    SELECT 3, observation_id, is_rg FROM research_grade_windowed(INTERVAL '14 days')
    UNION ALL
    SELECT 7, observation_id, is_rg FROM research_grade_windowed(INTERVAL '14 days')
    UNION ALL
    SELECT 14, observation_id, is_rg FROM research_grade_windowed(INTERVAL '14 days')
    UNION ALL
    SELECT 30, observation_id, is_rg FROM research_grade_windowed(INTERVAL '30 days')
    UNION ALL
    SELECT 60, observation_id, is_rg FROM research_grade_windowed(INTERVAL '60 days')
    UNION ALL
    SELECT 90, observation_id, is_rg FROM research_grade_windowed(INTERVAL '90 days')
    UNION ALL
    SELECT 180, observation_id, is_rg FROM research_grade_windowed(INTERVAL '6 months')
    UNION ALL
    SELECT 365, observation_id, is_rg FROM research_grade_windowed(INTERVAL '1 year')
    UNION ALL
    SELECT 540, observation_id, is_rg FROM research_grade_windowed(INTERVAL '18 months')
    UNION ALL
    SELECT 730, observation_id, is_rg FROM research_grade_windowed(INTERVAL '2 years')

)
SELECT
    w.days,
    COUNT(*) FILTER (WHERE f.eventually_rg AND w.is_rg)     AS true_rg_captured,
    COUNT(*) FILTER (WHERE f.eventually_rg)                 AS total_eventual_rg,
    ROUND(
        COUNT(*) FILTER (WHERE f.eventually_rg AND w.is_rg)::FLOAT
        / NULLIF(COUNT(*) FILTER (WHERE f.eventually_rg), 0), 3
    ) AS pct_captured
FROM settled_at_window w
JOIN settled_final f ON w.observation_id = f.observation_id
GROUP BY w.days
ORDER BY w.days
