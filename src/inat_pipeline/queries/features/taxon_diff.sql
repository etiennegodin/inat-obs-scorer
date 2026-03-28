-- Static features over dataset
CREATE OR REPLACE TABLE features.taxon_diff AS

WITH base_obs AS (
    SELECT
        observation_id,
        c.taxon_id,
        c.created_at,
        n_ids_at_window,
        score,
        consensus_level_rg,
        date_part('day', id_created_at - created_at)::INT AS time_to_rg_days,
        date_part('hour', id_created_at - created_at)::INT AS time_to_rg_hours,
        genus_id,
        family_id,
        order_id,

    FROM community_taxon_windowed(INTERVAL '999 years') c
    JOIN staged.taxa t ON c.taxon_id = t.taxon_id
),

from_ids AS (

    SELECT
        taxon_id,

        COUNT(DISTINCT(user_id)) / COUNT(user_id) AS specialist_identifer,

    FROM staged.identifications
    WHERE
        own_observation IS FALSE
        AND "current" IS TRUE

    GROUP BY taxon_id

),

from_obs AS (

    SELECT
        taxon_id,

        COUNT(DISTINCT(user_id)) / COUNT(user_id) AS specialist_observer,

    FROM staged.observations
    GROUP BY taxon_id

),

global_rate AS (

    SELECT AVG(consensus_level_rg::FLOAT) AS global_cm_rate,

    -- Species-level stats

    FROM base_obs

),

taxon_counts AS (
    -- First, get the raw count per taxon
    SELECT
        taxon_id,
        genus_id,
        family_id,
        order_id,

        AVG(time_to_rg_days) AS time_to_rg_mean,
        STDDEV(time_to_rg_days) AS time_to_rg_std,
        MEDIAN(time_to_rg_days) AS time_to_rg_med,
        AVG(n_ids_at_window) AS ids_rg_mean,
        STDDEV(n_ids_at_window) AS ids_rg_std,
        MEDIAN(n_ids_at_window) AS ids_rg_med,
        AVG(score) AS score_mean,
        STDDEV(score) AS score_std,
        MEDIAN(score) AS score_med,

        COUNT(*) AS taxon_obs_count,
        COUNT(*) FILTER (WHERE consensus_level_rg) AS taxon_cm_obs_count

    FROM base_obs
    GROUP BY ALL
),

aggregates AS (
    SELECT
        *,
        SUM(taxon_obs_count) OVER (PARTITION BY genus_id) AS genus_obs_count,
        SUM(taxon_obs_count) OVER (PARTITION BY family_id) AS family_obs_count,
        SUM(taxon_obs_count) OVER (PARTITION BY order_id) AS order_obs_count,

        SUM(taxon_cm_obs_count) OVER (PARTITION BY genus_id) AS genus_cm_obs,
        SUM(taxon_cm_obs_count) OVER (PARTITION BY family_id) AS family_cm_obs,
        SUM(taxon_cm_obs_count) OVER (PARTITION BY order_id) AS order_cm_obs,
    FROM taxon_counts
),

rates AS (

    SELECT
        *,

        --cm rate
        -- Raw rates at each level
        taxon_cm_obs_count::FLOAT / NULLIF(taxon_obs_count, 0) AS taxon_cm_rate_raw,
        genus_cm_obs::FLOAT / NULLIF(genus_obs_count, 0) AS genus_cm_rate,
        family_cm_obs::FLOAT / NULLIF(family_obs_count, 0) AS family_cm_rate,
        order_cm_obs::FLOAT / NULLIF(order_obs_count, 0) AS order_cm_rate,

        COALESCE(genus_cm_rate, family_cm_rate, order_cm_rate, g.global_cm_rate, 0.5) AS hierarchical_prior_cm,
        --shrinkage
        (10 * hierarchical_prior_cm + taxon_cm_obs_count) / (10 + taxon_obs_count) AS taxon_cm_rate_shrunk,
        g.global_cm_rate,
    FROM aggregates
    CROSS JOIN global_rate g

)

SELECT
    r.*,
    r.taxon_cm_rate_shrunk / r.global_cm_rate AS taxon_rate_global,
    i.specialist_identifer,
    ob.specialist_observer,

FROM rates r
LEFT JOIN from_ids i ON i.taxon_id = r.taxon_id
LEFT JOIN from_obs ob ON ob.taxon_id = r.taxon_id
