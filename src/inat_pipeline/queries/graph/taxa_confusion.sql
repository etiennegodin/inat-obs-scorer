CREATE OR REPLACE TABLE features.taxa_confusion AS

-- Single pass over observations, pre-aggregated to taxon level
WITH obs_stats AS (
    SELECT
        taxon_id,
        COUNT(DISTINCT uuid)                                                    AS obs_count,
        COALESCE(COUNT(DISTINCT uuid) FILTER (WHERE quality_grade = 'research'), 0) AS rg_count,
        COALESCE(ROUND(
            COUNT(DISTINCT uuid) FILTER (WHERE quality_grade = 'research')
            / NULLIF(COUNT(DISTINCT uuid), 0), 4), 0)                          AS rg_rate
    FROM staged.observations
    GROUP BY taxon_id
),


similar_species_agg AS(
    SELECT
        s.taxon_id,
        s.similar_taxon_id,
        COALESCE(o.obs_count, 0)                             AS similar_species_obs_count,
        COALESCE(o.rg_count,  0)                             AS similar_species_rg_count,
        COALESCE(o.rg_rate,   0)                             AS similar_species_rg_rate,
        d.taxonomic_distance,

        CASE
            WHEN tf.genus_id  = ts.genus_id  THEN 'same_genus'
            WHEN tf.family_id = ts.family_id THEN 'cross_genus'
            WHEN tf.order_id  = ts.order_id  THEN 'cross_family'
            ELSE                                  'cross_order'
        END AS confusion_boundary

    FROM staged.similar_species s
    LEFT JOIN obs_stats o          ON o.taxon_id  = s.similar_taxon_id
    JOIN  staged.taxa tf           ON tf.taxon_id = s.taxon_id
    JOIN  staged.taxa ts           ON ts.taxon_id = s.similar_taxon_id
    JOIN  features.taxa_distance d ON d.taxon_id  = s.taxon_id
                                   AND d.similar_taxon_id = s.similar_taxon_id

),
--Neighbor taxonomic spread
nbor_taxa_diversity AS(
    SELECT s.taxon_id,
        COUNT(DISTINCT(t.genus_id)) AS neighbor_genus_diversity,
        MIN(t.rank_level) neighbor_rank_min

    FROM staged.similar_species s
    JOIN staged.taxa t ON t.taxon_id = s.similar_taxon_id
    GROUP BY s.taxon_id
),
--aggregation over edges
aggregates AS(

    SELECT n.taxon_id::INT AS taxon_id,

        -- Focal species stats from pre-aggregated obs
        o.obs_count                                                     AS focal_species_obs_count,
        o.rg_count                                                      AS focal_species_rg_count,
        o.rg_rate,

        -- N neighbords
        COUNT(DISTINCT(n.similar_taxon_id)) AS similar_species_count,
        COUNT(DISTINCT n.similar_taxon_id) > 0                         AS has_similar_species,

        -- Neighbor obs aggregates
        SUM(n.similar_species_obs_count)                               AS nbor_obs_count_sum,
        ROUND(AVG(n.similar_species_obs_count), 2)                     AS nbor_obs_count_mean,
        ROUND(STDDEV(n.similar_species_obs_count), 5)                  AS nbor_obs_count_std,
        MAX(n.similar_species_obs_count)                               AS nbor_obs_count_max,

        -- Neighbor RG aggregates
        ROUND(AVG(n.similar_species_rg_rate), 4)                       AS nbor_rg_rate_mean,
        ROUND(STDDEV(n.similar_species_rg_rate), 5)                    AS nbor_rg_rate_std,
        MIN(n.similar_species_rg_rate)                                 AS nbor_rg_rate_min,

         -- Distance-weighted RG rate (far confusers weighted more)
        ROUND(AVG(n.similar_species_rg_rate * n.taxonomic_distance), 4)
                                                                        AS weighted_mean_neighbor_rg_rate,

        -- Inverse-distance weighted RG rate (near confusers weighted more)
        ROUND(
            SUM(n.similar_species_rg_rate / NULLIF(n.taxonomic_distance, 0))
            / NULLIF(SUM(1.0 / NULLIF(n.taxonomic_distance, 0)), 0),
        4)                                                              AS nbor_rg_rate_inv_dist_weighted,


        ROUND(
            (1 - AVG(n.similar_species_rg_rate * d.taxonomic_distance))
            * LOG(a.similar_species_count + 1),
        4) AS neighborhood_difficulty_dist_weighted,

        -- Taxonomic distance aggregates
        MAX(n.taxonomic_distance)                                      AS nbor_dist_max,
        ROUND(AVG(n.taxonomic_distance), 2)                            AS nbor_dist_mean,

        -- Relative standing
        o.rg_rate - ROUND(AVG(n.similar_species_rg_rate), 4)         AS rg_rate_vs_neighbors,

        -- Taxonomic boundary crossing
        COUNT(*) FILTER (WHERE confusion_boundary = 'same_genus')  AS nbor_count_same_genus,
        COUNT(*) FILTER (WHERE confusion_boundary = 'cross_genus') AS nbor_count_cross_genus,
        COUNT(*) FILTER (WHERE confusion_boundary = 'cross_family')AS nbor_count_cross_family,

        -- Fraction of confusers that cross genus boundary (0→1)
        ROUND(
            COUNT(*) FILTER (WHERE confusion_boundary != 'same_genus')::FLOAT
            / NULLIF(COUNT(*), 0),
        4) AS cross_genus_confusion_rate,

        -- Deepest boundary crossed (ordinal)
        MAX(CASE confusion_boundary
            WHEN 'same_genus'   THEN 1
            WHEN 'cross_genus'  THEN 2
            WHEN 'cross_family' THEN 3
            WHEN 'cross_order'  THEN 4
        END) AS max_confusion_boundary_crossed


    FROM similar_species_agg n
    -- obs stats from pre-aggregated table, single lookup
    JOIN obs_stats o ON o.taxon_id = n.taxon_id
    GROUP BY n.taxon_id, o.obs_count, o.rg_count, o.rg_rate

),
neighborhood_pool AS(

    -- Neighbors, attributed to their focal taxon's pool
    SELECT
        taxon_id AS focal_taxon_id,
        similar_taxon_id           AS pool_member_id,
        similar_species_rg_rate    AS rg_rate,
        taxonomic_distance,
        FALSE                      AS is_focal
    FROM similar_species_agg

    UNION ALL

    -- Focal taxon injected into its own pool
    SELECT
        taxon_id, taxon_id, rg_rate,
        0.0 AS taxonomic_distance,
        TRUE
    FROM aggregates
    WHERE taxon_id IN (SELECT DISTINCT taxon_id::INT FROM similar_species_agg)

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

        --COUNT(*) OVER (PARTITION BY focal_taxon_id) AS neighborhood_pool_size

    FROM neighborhood_pool
)

SELECT a.*,

    -- Taxon rg rate ranked in neighbors pool
    r.rg_percentile_in_neighborhood,
    r.rg_percentile_dist_weighted,
    --r.neighborhood_pool_size,

    n.*EXCLUDE(n.taxon_id),
    s.*EXCLUDE(s.taxon_id),

    -- Relative stand

    -- Summary statistic
    ROUND((1 - a.weighted_mean_neighbor_rg_rate) * LOG(a.similar_species_count + 1), 4) AS neighborhood_difficulty,
    ROUND((1 - a.nbor_rg_rate_inv_dist_weighted) * LOG(a.similar_species_count + 1), 4) AS neighborhood_difficulty_inv_dist


FROM aggregates a
JOIN nbor_taxa_diversity     n ON n.taxon_id = a.taxon_id
JOIN features.taxa_assymetry s ON s.taxon_id = a.taxon_id
JOIN ranked                  r ON r.focal_taxon_id = a.taxon_id
WHERE r.is_focal = TRUE
