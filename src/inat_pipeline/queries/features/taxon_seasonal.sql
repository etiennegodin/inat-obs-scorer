CREATE OR REPLACE TABLE taxon_month_stats AS

WITH params AS(
    SELECT
    10  AS alpha_taxon,   -- prior weight for species-level shrinkage
    20  AS alpha_monthly  -- higher alpha: fewer obs per cell

),

train_obs AS (
    SELECT
        o.taxon_id,
        t.genus_id,
        t.family_id,
        MONTH(o.observed_on)  AS obs_month,
        l.label AS is_rg
    FROM staged.observations o
    JOIN staged.taxa t ON o.taxon_id = t.taxon_id
    JOIN features.label l ON o.id = l.observation_id
    JOIN features.splits s ON o.id = s.observation_id
    CROSS JOIN params p

    WHERE s.split == 'train'
),
-- Species-level overall rate (your existing taxon feature — join in)
-- This becomes the prior for species × month shrinkage
taxon_overall AS (
    SELECT
        taxon_id,
        SUM(is_rg)::FLOAT / COUNT(*) AS taxon_rg_rate,
        COUNT(*) AS taxon_n
    FROM train_obs
    GROUP BY taxon_id
),

-- Species-level overall rate (your existing taxon feature — join in)
-- This becomes the prior for species × month shrinkage
-- Global monthly rate — fallback when taxon has no data for a given month
global_monthly AS (
    SELECT
        obs_month,
        SUM(is_rg)::FLOAT / COUNT(*) AS global_month_rg_rate
    FROM train_obs
    GROUP BY obs_month
),

-- Raw (taxon, month) counts
taxon_month_raw AS (
    SELECT
        taxon_id,
        obs_month,
        COUNT(*)       AS n,
        SUM(is_rg)     AS rg_count
    FROM train_obs
    GROUP BY taxon_id, obs_month
),

-- Shrinkage: prior is the taxon's own overall rate, not global mean
-- This decomposes the signal as: taxon_overall + seasonal_deviation
-- alpha_monthly is higher because cells are smaller
taxon_month_shrunk AS (
    SELECT
        tm.taxon_id,
        tm.obs_month,
        tm.n,
        -- Shrink toward taxon's own overall rate
        (tm.rg_count + p.alpha_monthly * t.taxon_rg_rate)
            / (tm.n + p.alpha_monthly)                     AS species_month_rg_rate,
        t.taxon_rg_rate                                    AS taxon_overall_rate,
        gm.global_month_rg_rate
    FROM taxon_month_raw tm
    JOIN taxon_overall t   USING (taxon_id)
    JOIN global_monthly gm USING (obs_month)
    CROSS JOIN params p
)

SELECT * FROM taxon_month_shrunk;

CREATE OR REPLACE TABLE taxon_peak_month AS
SELECT
    taxon_id,
    obs_month                          AS peak_month,
    species_month_rg_rate              AS peak_rg_rate
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY taxon_id
            ORDER BY species_month_rg_rate DESC
        ) AS rn
    FROM taxon_month_stats
)
WHERE rn = 1;
