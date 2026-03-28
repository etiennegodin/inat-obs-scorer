CREATE OR REPLACE TABLE features.taxon AS

WITH base_obs_raw AS (
    SELECT
        cm.observation_id,
        cm.created_at,
        cm.observed_on,
        cm.n_ids_at_window,
        cm.score,
        cm.consensus_level_rg,
        date_part('day', cm.created_at - cm.observed_on)::INT AS lag_days,
        date_part('day', id_created_at - created_at)::INT AS time_to_cm_days,
        CASE
            WHEN cm.community_taxon IS NULL THEN cm.taxon_id
            ELSE cm.community_taxon
        END AS taxon_id   -- ← this is now the stable key for everything below
    FROM community_taxon_windowed(INTERVAL '999 years') cm
    WHERE cm.created_at < :cutoff_date
),

-- Step 2: join taxa metadata on the RESOLVED taxon_id, not cm.taxon_id
base_obs AS (
    SELECT
        r.*,
        -- Self-referential fix: a genus row has no genus_id parent in staged.taxa,
        -- so coalesce it to itself so window partitions don't silently go to NULL
        COALESCE(
            t.genus_id,
            CASE WHEN t.rank_level = 20 THEN r.taxon_id END
        ) AS genus_id,
        COALESCE(
            t.family_id,
            CASE WHEN t.rank_level = 30 THEN r.taxon_id END
        ) AS family_id,
        COALESCE(
            t.order_id,
            CASE WHEN t.rank_level = 40 THEN r.taxon_id END
        ) AS order_id,
        t.rank_level
    FROM base_obs_raw r
    LEFT JOIN staged.taxa t ON r.taxon_id = t.taxon_id  -- ← correct key
),

identifiers AS (

    SELECT
        taxon_id,

        COUNT(DISTINCT(user_id)) / COUNT(user_id) AS specialist_identifer,

    FROM staged.identifications
    WHERE
        own_observation IS FALSE
        AND "current" IS TRUE

    GROUP BY taxon_id

),

observers AS (

    SELECT
        taxon_id,

        COUNT(DISTINCT(user_id)) / COUNT(user_id) AS specialist_observer,

    FROM staged.observations
    GROUP BY taxon_id

),

-- ── True medians: computed directly from base_obs so they're exact, not
--    medians-of-medians. One CTE per level to avoid a giant GROUP BY CUBE.
taxon_medians AS (
    SELECT
        taxon_id,
        MEDIAN(time_to_cm_days) AS taxon_time_to_cm_median,
        MEDIAN(n_ids_at_window) AS taxon_n_ids_median,
        MEDIAN(score) AS taxon_score_median
    FROM base_obs
    GROUP BY taxon_id
),

genus_medians AS (
    SELECT
        genus_id,
        MEDIAN(time_to_cm_days) AS genus_time_to_cm_median,
        MEDIAN(n_ids_at_window) AS genus_n_ids_median,
        MEDIAN(score) AS genus_score_median
    FROM base_obs
    GROUP BY genus_id
),

family_medians AS (
    SELECT
        family_id,
        MEDIAN(time_to_cm_days) AS family_time_to_cm_median,
        MEDIAN(n_ids_at_window) AS family_n_ids_median,
        MEDIAN(score) AS family_score_median
    FROM base_obs
    GROUP BY family_id
),

order_medians AS (
    SELECT
        order_id,
        MEDIAN(time_to_cm_days) AS order_time_to_cm_median,
        MEDIAN(n_ids_at_window) AS order_n_ids_median,
        MEDIAN(score) AS order_score_median
    FROM base_obs
    GROUP BY order_id
),

global_stats AS (
    SELECT
        AVG(consensus_level_rg::FLOAT) AS global_rg_rate,
        AVG(time_to_cm_days::FLOAT) AS global_time_to_cm_mean,
        AVG(n_ids_at_window::FLOAT) AS global_n_ids_mean,
        AVG(score) AS global_score_mean,
        MEDIAN(time_to_cm_days) AS global_time_to_cm_median,
        MEDIAN(n_ids_at_window) AS global_n_ids_median,
        MEDIAN(score) AS global_score_median
    FROM base_obs
),

-- ── Per-taxon aggregates (sums bubble up to higher levels via window functions)
taxon_counts AS (
    SELECT
        taxon_id,
        genus_id,
        family_id,
        order_id,
        rank_level,
        COUNT(*) AS taxon_obs_count,
        COUNT(*) FILTER (WHERE consensus_level_rg) AS taxon_rg_count,

        -- Sums needed to compute exact means at genus/family/order
        SUM(time_to_cm_days) AS taxon_time_to_cm_sum,
        SUM(n_ids_at_window) AS taxon_n_ids_sum,
        SUM(score) AS taxon_score_sum,

        -- Taxon-level means
        AVG(time_to_cm_days::FLOAT) AS taxon_time_to_cm_mean,
        AVG(n_ids_at_window::FLOAT) AS taxon_n_ids_mean,
        AVG(score) AS taxon_score_mean,

        taxon_rg_count::FLOAT / NULLIF(taxon_obs_count, 0) AS taxon_rg_rate_raw
    FROM base_obs
    GROUP BY taxon_id, genus_id, family_id, order_id, rank_level

),

-- ── Roll sums up to each taxonomic level with window functions
aggregates AS (
    SELECT
        tc.*,

        -- Observation counts
        SUM(taxon_obs_count) OVER (PARTITION BY genus_id) AS genus_obs_count,
        SUM(taxon_obs_count) OVER (PARTITION BY family_id) AS family_obs_count,
        SUM(taxon_obs_count) OVER (PARTITION BY order_id) AS order_obs_count,

        -- RG counts (for rate computation)
        SUM(taxon_rg_count) OVER (PARTITION BY genus_id) AS genus_rg_count,
        SUM(taxon_rg_count) OVER (PARTITION BY family_id) AS family_rg_count,
        SUM(taxon_rg_count) OVER (PARTITION BY order_id) AS order_rg_count,

        -- time_to_cm sums
        SUM(taxon_time_to_cm_sum) OVER (PARTITION BY genus_id) AS genus_time_to_cm_sum,
        SUM(taxon_time_to_cm_sum) OVER (PARTITION BY family_id) AS family_time_to_cm_sum,
        SUM(taxon_time_to_cm_sum) OVER (PARTITION BY order_id) AS order_time_to_cm_sum,

        -- n_ids sums
        SUM(taxon_n_ids_sum) OVER (PARTITION BY genus_id) AS genus_n_ids_sum,
        SUM(taxon_n_ids_sum) OVER (PARTITION BY family_id) AS family_n_ids_sum,
        SUM(taxon_n_ids_sum) OVER (PARTITION BY order_id) AS order_n_ids_sum,

        -- score sums
        SUM(taxon_score_sum) OVER (PARTITION BY genus_id) AS genus_score_sum,
        SUM(taxon_score_sum) OVER (PARTITION BY family_id) AS family_score_sum,
        SUM(taxon_score_sum) OVER (PARTITION BY order_id) AS order_score_sum
    FROM taxon_counts tc
),

-- ── Derive rates from sums (no name collisions — rg / time_to_cm / n_ids / score
--    are fully separate columns now)
rates AS (
    SELECT
        a.*,

        -- RG rates at each level
        genus_rg_count::FLOAT / NULLIF(genus_obs_count, 0) AS genus_rg_rate,
        family_rg_count::FLOAT / NULLIF(family_obs_count, 0) AS family_rg_rate,
        order_rg_count::FLOAT / NULLIF(order_obs_count, 0) AS order_rg_rate,

        -- time_to_cm means
        genus_time_to_cm_sum::FLOAT / NULLIF(genus_obs_count, 0) AS genus_time_to_cm_mean,
        family_time_to_cm_sum::FLOAT / NULLIF(family_obs_count, 0) AS family_time_to_cm_mean,
        order_time_to_cm_sum::FLOAT / NULLIF(order_obs_count, 0) AS order_time_to_cm_mean,

        -- n_ids means
        genus_n_ids_sum::FLOAT / NULLIF(genus_obs_count, 0) AS genus_n_ids_mean,
        family_n_ids_sum::FLOAT / NULLIF(family_obs_count, 0) AS family_n_ids_mean,
        order_n_ids_sum::FLOAT / NULLIF(order_obs_count, 0) AS order_n_ids_mean,

        -- score means (community taxon confidence, distinct from RG rate)
        genus_score_sum::FLOAT / NULLIF(genus_obs_count, 0) AS genus_score_mean,
        family_score_sum::FLOAT / NULLIF(family_obs_count, 0) AS family_score_mean,
        order_score_sum::FLOAT / NULLIF(order_obs_count, 0) AS order_score_mean
    FROM aggregates a
),

-- ── Bayesian shrinkage: prior is the lowest taxonomic level with enough observations.
--    COALESCE on rates is wrong here — a genus with n=2 gives a real (noisy) rate,
--    not NULL. Use a count threshold to decide when to trust each level.
bayesian AS (
    SELECT
        r.*,
        g.global_rg_rate,
        g.global_time_to_cm_mean,
        g.global_n_ids_mean,
        g.global_score_mean,

        -- Hierarchical prior: pick the lowest level with >= 10 obs
        CASE
            WHEN r.genus_obs_count >= 10 THEN r.genus_rg_rate
            WHEN r.family_obs_count >= 10 THEN r.family_rg_rate
            WHEN r.order_obs_count >= 10 THEN r.order_rg_rate
            ELSE g.global_rg_rate
        END AS hierarchical_prior_rg,

        CASE
            WHEN r.genus_obs_count >= 10 THEN 1
            WHEN r.family_obs_count >= 10 THEN 2
            WHEN r.order_obs_count >= 10 THEN 3
            ELSE 4
        END AS rg_rate_prior_source,

        CASE
            WHEN r.rank_level <= 10
                THEN  -- species or below
                    (10.0 * hierarchical_prior_rg + taxon_rg_count)
                    / (10.0 + taxon_obs_count)
            WHEN r.rank_level = 20
                THEN   -- genus-level observation
                    CASE
                        WHEN r.family_obs_count >= 10
                            THEN
                                (10.0 * r.family_rg_rate + taxon_rg_count)
                                / (10.0 + taxon_obs_count)
                        WHEN r.order_obs_count >= 10
                            THEN
                                (10.0 * r.order_rg_rate + taxon_rg_count)
                                / (10.0 + taxon_obs_count)
                        ELSE
                            (10.0 * g.global_rg_rate + taxon_rg_count)
                            / (10.0 + taxon_obs_count)
                    END
            ELSE  -- family or above: shrink toward order
                CASE
                    WHEN r.order_obs_count >= 10
                        THEN
                            (10.0 * r.order_rg_rate + taxon_rg_count)
                            / (10.0 + taxon_obs_count)
                    ELSE
                        (10.0 * g.global_rg_rate + taxon_rg_count)
                        / (10.0 + taxon_obs_count)
                END
        END AS effective_rg_rate_shrunk,

        -- Same pattern for time_to_cm and n_ids means
        CASE
            WHEN r.rank_level <= 10 THEN r.taxon_time_to_cm_mean
            WHEN r.rank_level = 20 THEN r.family_time_to_cm_mean
            ELSE r.order_time_to_cm_mean
        END AS effective_time_to_cm_mean,

        CASE
            WHEN r.rank_level <= 10 THEN r.taxon_n_ids_mean
            WHEN r.rank_level = 20 THEN r.family_n_ids_mean
            ELSE r.order_n_ids_mean
        END AS effective_n_ids_mean,

        CASE
            WHEN r.rank_level <= 10 THEN r.taxon_score_mean
            WHEN r.rank_level = 20 THEN r.family_score_mean
            ELSE r.order_score_mean
        END AS effective_score_mean,

        -- Popularity (log-scale observation count)
        LOG(taxon_obs_count + 1) AS taxon_popularity_log,
        LOG(genus_obs_count + 1) AS genus_popularity_log,
        LOG(family_obs_count + 1) AS family_popularity_log,
        LOG(order_obs_count + 1) AS order_popularity_log,

        taxon_obs_count < 30 AS taxon_cold_start
    FROM rates r
    CROSS JOIN global_stats g
)

SELECT
    b.*,

    -- Medians (joined from dedicated per-group CTEs — true medians, not median-of-medians)

    CASE
        WHEN b.rank_level <= 10 THEN tm.taxon_time_to_cm_median
        WHEN b.rank_level = 20 THEN gm.genus_time_to_cm_median
        WHEN b.rank_level = 20 THEN fm.family_time_to_cm_median
        ELSE om.order_time_to_cm_median
    END AS effective_time_to_cm_median,

    CASE
        WHEN b.rank_level <= 10 THEN rg_rate_prior_source
        WHEN b.rank_level = 20 THEN rg_rate_prior_source + 1  -- one level up
        ELSE rg_rate_prior_source + 2
    END AS effective_prior_source,

    tm.taxon_time_to_cm_median,
    tm.taxon_n_ids_median,
    tm.taxon_score_median,

    gm.genus_time_to_cm_median,
    gm.genus_n_ids_median,
    gm.genus_score_median,

    fm.family_time_to_cm_median,
    fm.family_n_ids_median,
    fm.family_score_median,

    om.order_time_to_cm_median,
    om.order_n_ids_median,
    om.order_score_median,

    -- Global medians for fallback / normalisation
    g.global_time_to_cm_median,
    g.global_n_ids_median,
    g.global_score_median,

    i.specialist_identifer,
    ob.specialist_observer,

    -- Taxonomy labels
    t.phylum,
    t.class,
    t."order",
    t.family,
    t.genus,
    t.species

FROM bayesian b
LEFT JOIN taxon_medians tm ON b.taxon_id = tm.taxon_id
LEFT JOIN genus_medians gm ON b.genus_id = gm.genus_id
LEFT JOIN family_medians fm ON b.family_id = fm.family_id
LEFT JOIN order_medians om ON b.order_id = om.order_id
LEFT JOIN identifiers i ON b.order_id = i.taxon_id
LEFT JOIN observers ob ON b.order_id = ob.taxon_id

CROSS JOIN global_stats g
LEFT JOIN staged.taxa t ON b.taxon_id = t.taxon_id
