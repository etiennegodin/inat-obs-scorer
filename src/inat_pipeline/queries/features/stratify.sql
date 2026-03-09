CREATE OR REPLACE TABLE features.splits AS

WITH base AS(
    SELECT 
        observation_id,
        created_at,
        submitted_year
    FROM features.observations 
),

pre_cutoff AS(

SELECT *,
'train' AS split
FROM base
WHERE created_at <= DATE('2021-01-01')
),

post_cutoff_gapped AS (
    SELECT *
    FROM   base
    WHERE  created_at >= DATE('2021-01-01')
      AND  created_at NOT BETWEEN
               DATE '2021-01-01' AND DATE '2021-01-01' + INTERVAL '90 days'
      AND  created_at NOT BETWEEN
               DATE '2022-06-01' AND DATE '2022-06-01' + INTERVAL '90 days'
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
               WHEN rn <= FLOOR(year_total * 0.70) THEN 'train'
               WHEN rn <= FLOOR(year_total * 0.85) THEN 'val'
               ELSE                                     'test'
           END AS split
    FROM   post_cutoff_ranked
)

SELECT observation_id, split
FROM   pre_cutoff

UNION ALL

SELECT observation_id, split
FROM   post_cutoff_split;