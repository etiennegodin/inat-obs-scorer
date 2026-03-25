CREATE OR REPLACE TABLE features.splits AS

WITH params AS (
    SELECT
        CAST(:cutoff_date AS DATE) AS cutoff_date,
        CAST(:val_start AS DATE) AS val_start,    -- cutoff + gap
        CAST(:val_end AS DATE) AS val_end,    -- cutoff + gap
        CAST(:test_start AS DATE) AS test_start,   -- val_end + gap
        :max_val_size AS max_val_size,
        :max_test_size AS max_test_size

),

base AS (
    SELECT
        observation_id,
        created_at,
        submitted_year
    FROM features.model_population
),

-- Everything before cutoff is train.
train_obs AS (
    SELECT
        observation_id,
        'train' AS split
    FROM base, params p
    WHERE created_at < p.cutoff_date
),

-- Val: temporally after the gap. Downsample if too large,
-- but preserve temporal order within the sample.
val_obs AS (
    SELECT
        observation_id,
        'val' AS split
    FROM (
        SELECT
            observation_id,
            ROW_NUMBER() OVER (ORDER BY created_at, MD5(observation_id::TEXT)) AS rn
        FROM base, params p
        WHERE
            created_at >= p.val_start
            AND created_at < p.val_end
    ) ranked, params p
    WHERE rn <= p.max_val_size
),

-- Test: temporally after val gap. Same downsampling logic.
test_obs AS (
    SELECT
        observation_id,
        'test' AS split
    FROM (
        SELECT
            observation_id,
            ROW_NUMBER() OVER (ORDER BY created_at, MD5(observation_id::TEXT)) AS rn
        FROM base, params p
        WHERE created_at >= p.test_start
    ) ranked, params p
    WHERE rn <= p.max_test_size
)

SELECT * FROM train_obs
UNION ALL
SELECT * FROM val_obs
UNION ALL
SELECT * FROM test_obs;
