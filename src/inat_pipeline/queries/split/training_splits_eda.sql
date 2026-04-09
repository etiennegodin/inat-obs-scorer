-- Check 2: Split size and label rate per split

CREATE SCHEMA IF NOT EXISTS eda;
CREATE OR REPLACE TABLE eda.splits AS

WITH base AS (
    SELECT
        b.observation_id,
        b.created_at,
        DATE_TRUNC('month', b.created_at)::DATE AS month,
        COALESCE(l.label, FALSE) AS label,
        s.split AS split

    FROM features.base b
    -- Only model population (no-id filter already applied)
    JOIN features.model_population mp ON mp.observation_id = b.observation_id
    LEFT JOIN features.label l ON l.observation_id = b.observation_id
    LEFT JOIN features.splits s ON s.observation_id = b.observation_id
),

classified AS (
    SELECT
        month,
        label,
        CASE
            WHEN split IS NOT NULL THEN split
            -- Dropped: in the timeline but not assigned to any split
            -- (gap buffer or scrape floor casualty)
            ELSE 'dropped'
        END AS bucket
    FROM base
)

SELECT
    month,
    bucket,
    COUNT(*) AS n_obs,
    ROUND(AVG(label::INT), 4) AS pos_rate
FROM classified
GROUP BY month, bucket
ORDER BY month, bucket;


SELECT
    CASE
        WHEN s.split == 'train' THEN 0
        WHEN s.split == 'val' THEN 1
        WHEN s.split == 'test' THEN 2
        ELSE NULL
    END AS idx,
    s.split,

    COUNT(s.observation_id) AS n_obs,
    ROUND(AVG(l.label::FLOAT), 3) AS pos_rate

FROM features.splits s
JOIN features.label l on s.observation_id = l.observation_id
GROUP BY split
ORDER BY idx ASC;
