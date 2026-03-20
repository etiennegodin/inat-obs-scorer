CREATE OR REPLACE TABLE features.taxon AS


WITH base AS (
    SELECT
        rg.observation_id,
        -- use computed taxon_id first, if null fallback to observation_id
        CASE
            WHEN rg.community_taxon_id IS NULL THEN rg.taxon_id
            ELSE rg.community_taxon_id
        END AS taxon_id,
        CASE
            WHEN rg.community_taxon_id IS NULL THEN 'taxon_id'
            ELSE 'community_taxon'
        END AS taxon_id_source,
        rg.created_at,
        rg.is_rg,
        rg.n_ids_at_window,
        t.genus_id,
        t.family_id,
        t.order_id,
        t.rank_level
    FROM research_grade_windowed(INTERVAL '999 years') rg
    LEFT JOIN staged.taxa t ON rg.taxon_id = t.taxon_id
),

aggregates AS(

    SELECT
        observation_id,
        taxon_id,
        taxon_id_source,
        created_at,
        genus_id,
        family_id,
        order_id,
        rank_level,
        is_rg,

    -- Species-level stats
    COALESCE(COUNT(*)                   OVER taxon_history, 0)  AS taxon_obs_count,
    COALESCE(SUM(is_rg::INT)            OVER taxon_history, 0)  AS taxon_rg_obs,

    -- Genus-level stats (partition by genus_id)
    COALESCE(COUNT(*)                   OVER genus_history, 0)  AS genus_obs_count,
    COALESCE(SUM(is_rg::INT)            OVER genus_history, 0)  AS genus_rg_obs,

    -- Family-level stats
    COALESCE(COUNT(*)                   OVER family_history, 0) AS family_obs_count,
    COALESCE(SUM(is_rg::INT)            OVER family_history, 0) AS family_rg_obs,

    -- Order-level stats (last resort)
    COALESCE(COUNT(*)                   OVER order_history, 0)  AS order_obs_count,
    COALESCE(SUM(is_rg::INT)            OVER order_history, 0)  AS order_rg_obs,

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
        taxon_history  AS (PARTITION BY taxon_id  ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        genus_history  AS (PARTITION BY genus_id  ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        family_history AS (PARTITION BY family_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        order_history  AS (PARTITION BY order_id  ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
),

rates AS(

    SELECT *,

    -- Raw rates at each level
    taxon_rg_obs::FLOAT  / NULLIF(taxon_obs_count, 0)  AS taxon_rg_rate_raw,
    genus_rg_obs::FLOAT  / NULLIF(genus_obs_count, 0)  AS genus_rg_rate,
    family_rg_obs::FLOAT / NULLIF(family_obs_count, 0) AS family_rg_rate,
    order_rg_obs::FLOAT  / NULLIF(order_obs_count, 0)  AS order_rg_rate,


    -- Hierarchical prior for shrinkage
    -- 0.5 cold start fallback
    COALESCE(genus_rg_rate, family_rg_rate, order_rg_rate, global_rg_rate, 0.5) AS hierarchical_prior,

    (10 * hierarchical_prior + taxon_rg_obs) / (10 + taxon_obs_count) AS taxon_rg_rate_shrunk,

    -- Which level actually provided the rate
    /*
    CASE
        WHEN taxon_obs_count  >= 30 THEN 'species'
        WHEN genus_obs_count  >= 30 THEN 'genus'
        WHEN family_obs_count >= 30 THEN 'family'
        WHEN order_obs_count  >= 30 THEN 'order'
        ELSE 'insufficient'
    END AS rg_rate_source,


    COALESCE(taxon_rg_rate_shrunk, 0) AS taxon_rg_rate_shrunk_safe,
    CASE rg_rate_source
        WHEN 'species' THEN taxon_obs_count
        WHEN 'genus'   THEN genus_obs_count
        WHEN 'family'  THEN family_obs_count
        WHEN 'order'   THEN order_obs_count
    END AS source_obs_count,
    */

    CASE WHEN taxon_obs_count  < 30 THEN TRUE ELSE FALSE END AS taxon_cold_start,

    -- Popularity of taxon hierarchy at this time
    LOG(taxon_obs_count + 1) AS taxon_popularity_rank,
    LOG(genus_obs_count + 1) AS genus_popularity_rank,
    LOG(family_obs_count + 1) AS family_popularity_rank,
    LOG(order_obs_count + 1) AS order_popularity_rank,

    FROM aggregates

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
LEFT JOIN staged.taxa t ON r.taxon_id = t.taxon_id;
