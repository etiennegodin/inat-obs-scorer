CREATE OR REPLACE TABLE features.taxa_confusion AS

WITH config AS (

    SELECT SUM(is_rg) / COUNT(*) OVER () as global_rg_rate
    FROM research_grade_windowed(INTERVAL '999 years')

),

obs_counts AS (
    SELECT
        taxon_id,
        COUNT(DISTINCT observation_id) AS obs_count,
        COUNT(observation_id) FILTER (WHERE is_rg) AS rg_count,
        MEDIAN(media_time_to_rg_days) AS time_to_rg_days_median,
        AVG(media_time_to_rg_days) AS time_to_rg_days_mean

    FROM research_grade_windowed(INTERVAL '999 years')
    WHERE created_at < :cutoff_date

    GROUP BY taxon_id
),

obs_stats AS (

    SELECT
        *,
        rg_count / NULLIF(obs_count, 0) AS rg_rate,

        -- dynamic alpha
        CASE
            WHEN obs_count < 6 THEN 15   -- high shrinkage
            WHEN obs_count < 50 THEN 5
            ELSE 2    -- trust the data
        END AS alpha,

    FROM obs_counts

),

similar_species_agg AS (
    SELECT
        s.taxon_id,
        s.similar_taxon_id,
        o.alpha,
        o.obs_count AS similar_species_obs_count,
        o.rg_count AS similar_species_rg_count,
        o.rg_rate AS similar_species_rg_rate_raw,
        d.taxonomic_distance,
        o.time_to_rg_days_mean AS similar_species_time_to_rg_mean,
        o.time_to_rg_days_median AS similar_species_time_to_rg_median,
        (o.obs_count * o.rg_rate + o.alpha * c.global_rg_rate)
        / NULLIF(o.obs_count + o.alpha, 0) AS similar_species_rg_rate_shrunk_global,

    FROM staged.similar_species s
    LEFT JOIN obs_stats o ON o.taxon_id = s.similar_taxon_id

    JOIN
        staged.taxa_distance
            d
        ON
            d.taxon_id = s.taxon_id
            AND d.similar_taxon_id = s.similar_taxon_id
    CROSS JOIN config c

),

pool_mean_agg AS (
    SELECT
        *,
        (
            SUM(similar_species_rg_rate_raw) OVER (PARTITION BY taxon_id)
            - similar_species_rg_rate_raw
        )
        / NULLIF(COUNT(*) OVER (PARTITION BY taxon_id) - 1, 0) AS pool_mean,

        (
            SUM(similar_species_time_to_rg_mean) OVER (PARTITION BY taxon_id)
            - similar_species_time_to_rg_mean
        )
        / NULLIF(COUNT(*) OVER (PARTITION BY taxon_id) - 1, 0) AS time_to_rg_pool_mean

    FROM similar_species_agg
),

shrunk_toward_neighborhood AS (
    SELECT
        *,

        (
            similar_species_obs_count * similar_species_rg_rate_raw
            + alpha * pool_mean
        )
        / NULLIF(similar_species_obs_count + alpha, 0)
            AS similar_species_rg_rate_shrunk_nbor
    FROM pool_mean_agg
),

--aggregation over edges
aggregates AS (

    SELECT
        n.taxon_id::INT AS taxon_id,

        -- Focal species stats from pre-aggregated obs
        o.obs_count AS focal_species_obs_count,
        o.rg_count AS focal_species_rg_count,
        o.rg_rate,
        t.similar_species_count,

        -- Neighbor obs aggregates
        SUM(n.similar_species_obs_count) AS nbor_obs_count_sum,
        AVG(n.similar_species_obs_count) AS nbor_obs_count_mean,
        MEDIAN(n.similar_species_obs_count) AS nbor_obs_count_median,

        STDDEV(n.similar_species_obs_count) AS nbor_obs_count_std,
        MAX(n.similar_species_obs_count) AS nbor_obs_count_max,

        -- Neighbor RG aggregates
        AVG(n.similar_species_rg_rate_shrunk_nbor) AS nbor_rg_rate_mean,
        STDDEV(n.similar_species_rg_rate_shrunk_nbor) AS nbor_rg_rate_std,
        MIN(n.similar_species_rg_rate_shrunk_nbor) AS nbor_rg_rate_min,
        MEDIAN(n.similar_species_rg_rate_shrunk_nbor) AS nbor_rg_rate_median,

        -- Distance-weighted RG rate (far confusers weighted more)
        AVG(n.similar_species_rg_rate_shrunk_nbor * n.taxonomic_distance)
            AS weighted_mean_neighbor_rg_rate,

        -- Inverse-distance weighted RG rate (near confusers weighted more)

        SUM(n.similar_species_rg_rate_shrunk_nbor / NULLIF(n.taxonomic_distance, 0))
        / NULLIF(SUM(1.0 / NULLIF(n.taxonomic_distance, 0)), 0) AS nbor_rg_rate_inv_dist_weighted,

        (1 - AVG(n.similar_species_rg_rate_shrunk_nbor * n.taxonomic_distance))
        * LOG(t.similar_species_count) AS neighborhood_difficulty_dist_weighted,

        -- Relative standing
        o.rg_rate - AVG(n.similar_species_rg_rate_shrunk_nbor) AS rg_rate_vs_neighbors,

        -- Neighbor time-to-RG aggregates

        ROUND(AVG(n.similar_species_time_to_rg_mean), 2) AS nbor_time_to_rg_mean,
        ROUND(STDDEV(n.similar_species_time_to_rg_mean), 4) AS nbor_time_to_rg_std,
        MAX(n.similar_species_time_to_rg_mean) AS nbor_time_to_rg_max,
        MIN(n.similar_species_time_to_rg_mean) AS nbor_time_to_rg_min,

        -- Distance-weighted: nearby slow-resolvers are more diagnostic
        ROUND(
            SUM(n.similar_species_time_to_rg_mean / NULLIF(n.taxonomic_distance, 0))
            / NULLIF(SUM(1.0 / NULLIF(n.taxonomic_distance, 0)), 0),
            2
        ) AS nbor_time_to_rg_inv_dist_weighted,

        -- Relative standing: does focal resolve faster or slower than its neighborhood?
        -- Positive = focal is faster than neighbors (easier)
        o.time_to_rg_days_mean - ROUND(AVG(n.similar_species_time_to_rg_mean), 2)
            AS time_to_rg_vs_neighbors,

    FROM shrunk_toward_neighborhood n
    -- obs stats from pre-aggregated table, single lookup
    JOIN obs_stats o ON o.taxon_id = n.taxon_id
    JOIN graph.confusion_topology t ON t.taxon_id = n.taxon_id
    GROUP BY n.taxon_id, o.obs_count, o.rg_count, o.rg_rate, t.similar_species_count,

),

neighborhood_pool AS (

    -- Neighbors, attributed to their focal taxon's pool
    SELECT
        taxon_id AS focal_taxon_id,
        similar_taxon_id AS pool_member_id,
        similar_species_rg_rate_shrunk_nbor AS rg_rate,
        taxonomic_distance,
        FALSE AS is_focal
    FROM shrunk_toward_neighborhood

    UNION ALL

    -- Focal taxon injected into its own pool
    SELECT
        taxon_id,
        taxon_id,
        rg_rate,
        0.0 AS taxonomic_distance,
        TRUE
    FROM aggregates
    WHERE taxon_id IN (SELECT DISTINCT taxon_id::INT FROM shrunk_toward_neighborhood)

),

ranked AS (
    SELECT
        *,
        PERCENT_RANK() OVER (
            PARTITION BY focal_taxon_id
            ORDER BY rg_rate ASC          -- 0.0 = hardest in pool, 1.0 = easiest
        ) AS rg_percentile_in_neighborhood,

        -- Distance-weighted rank: nearby confusers penalize more
        PERCENT_RANK() OVER (
            PARTITION BY focal_taxon_id
            ORDER BY (rg_rate * (1.0 / (1.0 + taxonomic_distance))) ASC
        ) AS rg_percentile_dist_weighted,

        COUNT(*) OVER (PARTITION BY focal_taxon_id) AS neighborhood_pool_size

    FROM neighborhood_pool
)

SELECT
    a.*,

    -- Taxon rg rate ranked in neighbors pool
    r.rg_percentile_in_neighborhood,
    r.rg_percentile_dist_weighted,
    r.neighborhood_pool_size,

    -- Relative stand

    -- Summary statistic
    (1 - a.nbor_rg_rate_inv_dist_weighted) * LOG(a.similar_species_count + 1) AS neighborhood_difficulty_inv_dist

FROM aggregates a
JOIN ranked r ON r.focal_taxon_id = a.taxon_id
WHERE r.is_focal = TRUE
