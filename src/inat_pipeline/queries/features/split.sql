CREATE OR REPLACE TABLE tests.splits AS

WITH params AS (
    SELECT
        CAST(:cutoff_date AS DATE)                     AS cutoff_date,
        :gap_days                        AS gap_days,
        CAST(:train_val_boundary AS DATE)             AS train_val_boundary,
        CAST(:val_test_boundary AS DATE)                     AS val_test_boundary,
        :train_frac                        AS train_frac,
        :val_frac                         AS val_frac,
        train_frac + val_frac      AS train_val_frac

),

base AS(
    SELECT
        observation_id,
        created_at,
        submitted_year
    FROM features.observations,
),

pre_cutoff AS(

SELECT *,
'train' AS split
FROM base, params p
WHERE created_at <= p.cutoff_date
),

post_cutoff_gapped AS (
    SELECT *
    FROM   base, params p
    WHERE  created_at >= p.cutoff_date
      AND  created_at NOT BETWEEN
               p.train_val_boundary AND p.train_val_boundary + INTERVAL '90 days'
      AND  created_at NOT BETWEEN
               p.val_test_boundary AND p.val_test_boundary + INTERVAL '90 days'
),

-- Step 2c: Stratified sampling within each post-cutoff year
--          Assign a row number within each year, then bucket by rank
post_cutoff_ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY submitted_year
               ORDER BY     MD5(observation_id::TEXT)   -- deterministic shuffle
           )                              AS rn,
           COUNT(*) OVER (
               PARTITION BY submitted_year
           )                              AS year_total
    FROM   post_cutoff_gapped
),

post_cutoff_split AS (
    SELECT *,
           CASE
               WHEN rn <= FLOOR(year_total * p.train_frac) THEN 'train'
               WHEN rn <= FLOOR(year_total * p.train_val_frac) THEN 'val'
               ELSE                                     'test'
           END AS split
    FROM   post_cutoff_ranked, params p
)

SELECT observation_id, split
FROM   pre_cutoff

UNION ALL

SELECT observation_id, split
FROM   post_cutoff_split;
