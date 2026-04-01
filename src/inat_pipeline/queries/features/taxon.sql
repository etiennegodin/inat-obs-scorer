CREATE OR REPLACE TABLE features.taxon AS

WITH config AS (
    SELECT
        to_days(:label_window_days) AS window_val,
        5.0 AS alpha

),

base AS (
    SELECT
        rg.observation_id,
        rg.created_at,
        rg.observed_on,
        rg.is_rg,
        rg.n_ids_at_window,
        t.genus_id,
        t.family_id,
        t.order_id,
        t.rank_level,
        date_part('day', rg.created_at - rg.observed_on)::INT AS lag_days,
        rg.time_to_rg_days,
        rg.taxon_id

    FROM research_grade_windowed((SELECT window_val FROM config)) rg
    LEFT JOIN staged.taxa t ON rg.taxon_id = t.taxon_id
),

aggregates AS (

    SELECT
        observation_id,
        taxon_id,
        created_at,
        genus_id,
        family_id,
        order_id,
        rank_level,
        is_rg,

        -- Observation - submission lag median

        quantile_cont(lag_days, 0.5) OVER taxon_history AS taxon_lag_days_median,
        AVG(lag_days) OVER taxon_history AS taxon_lag_days_mean,
        MAX(lag_days) OVER taxon_history AS taxon_lag_days_max,

        quantile_cont(lag_days, 0.5) OVER genus_history AS genus_lag_days_median,
        AVG(lag_days) OVER genus_history AS genus_lag_days_mean,
        MAX(lag_days) OVER genus_history AS genus_lag_days_max,

        quantile_cont(lag_days, 0.5) OVER family_history AS family_lag_days_median,
        AVG(lag_days) OVER family_history AS family_lag_days_mean,
        MAX(lag_days) OVER family_history AS family_lag_days_max,

        -- Tiem to rg
        AVG(time_to_rg_days) FILTER (WHERE is_rg) OVER taxon_history AS taxon_time_to_rg_mean,
        AVG(time_to_rg_days) FILTER (WHERE is_rg) OVER genus_history AS genus_time_to_rg_mean,
        AVG(time_to_rg_days) FILTER (WHERE is_rg) OVER family_history AS family_time_to_rg_mean,

        STDDEV(time_to_rg_days) FILTER (WHERE is_rg) OVER taxon_history AS taxon_time_to_rg_std,
        STDDEV(time_to_rg_days) FILTER (WHERE is_rg) OVER genus_history AS genus_time_to_rg_std,
        STDDEV(time_to_rg_days) FILTER (WHERE is_rg) OVER family_history AS family_time_to_rg_std,
        -- Species-level stats
        COALESCE(COUNT(*) OVER taxon_history, 0) AS taxon_obs_count,
        COALESCE(SUM(is_rg::INT) OVER taxon_history, 0) AS taxon_rg_obs,

        -- Genus-level stats (partition by genus_id)
        COALESCE(COUNT(*) OVER genus_history, 0) AS genus_obs_count,
        COALESCE(SUM(is_rg::INT) OVER genus_history, 0) AS genus_rg_obs,

        -- Family-level stats
        COALESCE(COUNT(*) OVER family_history, 0) AS family_obs_count,
        COALESCE(SUM(is_rg::INT) OVER family_history, 0) AS family_rg_obs,

        -- Order-level stats (last resort)
        COALESCE(COUNT(*) OVER order_history, 0) AS order_obs_count,
        COALESCE(SUM(is_rg::INT) OVER order_history, 0) AS order_rg_obs,

        -- ID convergence tendency
        COALESCE(
            AVG(n_ids_at_window) FILTER (WHERE is_rg) OVER taxon_history, 0
        ) AS taxon_avg_ids_to_rg,

        -- Global prior
        AVG(is_rg::FLOAT) OVER (
            ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS global_rg_rate,

    FROM base

    WINDOW
        taxon_history AS (PARTITION BY taxon_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        genus_history AS (PARTITION BY genus_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        family_history AS (PARTITION BY family_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        order_history AS (PARTITION BY order_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
),

rates AS (

    SELECT
        * EXCLUDE (global_rg_rate),

        -- Raw rates at each level
        taxon_rg_obs::FLOAT / NULLIF(taxon_obs_count, 0) AS taxon_rg_rate_raw,
        genus_rg_obs::FLOAT / NULLIF(genus_obs_count, 0) AS genus_rg_rate,
        family_rg_obs::FLOAT / NULLIF(family_obs_count, 0) AS family_rg_rate,
        order_rg_obs::FLOAT / NULLIF(order_obs_count, 0) AS order_rg_rate,

        -- Hierarchical prior for shrinkage
        COALESCE(
            CASE WHEN genus_obs_count >= 20 THEN genus_rg_rate END,
            CASE WHEN family_obs_count >= 50 THEN family_rg_rate END,
            CASE WHEN order_obs_count >= 500 THEN order_rg_rate END,
            global_rg_rate,
            0.5
        ) AS hierarchical_prior,
        (c.alpha * hierarchical_prior + taxon_rg_obs) / (c.alpha + taxon_obs_count) AS taxon_rg_rate_shrunk,
        global_rg_rate,

        -- time to rg fallback
        COALESCE(taxon_time_to_rg_mean, genus_time_to_rg_mean, family_time_to_rg_mean) AS effective_time_to_rg_mean,
        COALESCE(taxon_time_to_rg_std, genus_time_to_rg_std, family_time_to_rg_std) AS effective_time_to_rg_std,

        -- Which level actually provided the prior
        CASE
            WHEN genus_obs_count >= 20 THEN 1
            WHEN family_obs_count >= 50 THEN 2  -- was: family_rg_rate > 0
            WHEN order_obs_count >= 500 THEN 3
            ELSE 4
        END AS rg_rate_prior_source,

        -- Flag cold start if few observations
        CASE WHEN taxon_obs_count < 6 THEN TRUE ELSE FALSE END AS taxon_cold_start, -- median

        -- Popularity of taxon hierarchy at this time
        LOG(taxon_obs_count + 1) AS taxon_popularity_rank,
        LOG(genus_obs_count + 1) AS genus_popularity_rank,
        LOG(family_obs_count + 1) AS family_popularity_rank,
        LOG(order_obs_count + 1) AS order_popularity_rank,

    FROM aggregates
    CROSS JOIN config c

)

SELECT
    r.*,
    t.phylum,
    t.class,
    t."order",
    t.family,
    t.genus,
    t.species

FROM rates r
LEFT JOIN staged.taxa t ON r.taxon_id = t.taxon_id
ORDER BY observation_id;
