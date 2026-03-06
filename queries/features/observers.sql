CREATE OR REPLACE TABLE features.obs AS
SELECT
    o.id,
    o.user_id,
    o.created_at,
    o.quality_grade,

--o.created_at - u.created_at as observer_tenure_days,


-- Observer stats at time T (excluding current observation)
COUNT(*) OVER observer_history
    AS observer_obs_count_at_t,

SUM(CASE WHEN quality_grade = 'research' THEN 1 ELSE 0 END) OVER observer_history
    AS observer_rg_count_at_t,

AVG(CASE WHEN quality_grade = 'research' THEN 1.0 ELSE 0 END) OVER observer_history
    AS observer_rg_rate_at_t,


-- Observer 12months stats at time T 
COUNT(*) OVER observer_12m
        AS observer_obs_count_12m,

SUM(CASE WHEN quality_grade = 'research' THEN 1.0 ELSE 0 END) OVER observer_12m
    AS observer_rg_count_12m,

SUM(CASE WHEN quality_grade = 'research' THEN 1.0 ELSE 0 END) OVER observer_12m
/ NULLIF(COUNT(*) OVER observer_12m, 0)
    AS observer_rg_rate_12m,


FROM staged.observations o
JOIN staged.users u ON o.user_id = u.user_id

WINDOW
    observer_history AS (
        PARTITION BY o.user_id
        ORDER BY o.created_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ),
    observer_12m AS (
        PARTITION BY o.user_id
        ORDER BY o.created_at
        RANGE BETWEEN INTERVAL 12 MONTHS PRECEDING AND INTERVAL 1 MICROSECOND PRECEDING
    )