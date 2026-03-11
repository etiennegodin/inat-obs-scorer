CREATE OR REPLACE TABLE features.taxa_confusion AS

--Neighbors count
WITH similar_species_agg AS(
    SELECT
        s.taxon_id,
        s.similar_taxon_id,
        COUNT(DISTINCT(o.uuid))AS similar_species_obs_count,
        COALESCE(COUNT(DISTINCT(o.uuid)) FILTER (WHERE o.quality_grade == 'research'),0) AS similar_species_rg_count,
        COALESCE(
            ROUND(similar_species_rg_count / NULLIF(similar_species_obs_count,0),4),
            0) AS similar_species_rg_rate

    FROM staged.similar_species s
    LEFT JOIN staged.observations o ON o.taxon_id = s.similar_taxon_id
    GROUP BY s.similar_taxon_id, s.taxon_id
),
--Neighbor taxonomic spread
nbor_taxa_diversity AS(
    SELECT s.taxon_id,
        COUNT(DISTINCT(t.genus_id)) AS neighbor_genus_diversity,
        MIN(t.rank_level) neighbor_rank_min

    FROM staged.similar_species s
    JOIN staged.taxa t ON s.taxon_id = t.taxon_id
    GROUP BY s.taxon_id
),

aggregates AS(

    SELECT s.taxon_id::INT AS taxon_id,

        -- obs focal species
        COUNT(DISTINCT(o.uuid))AS focal_species_obs_count,
        COALESCE(COUNT(DISTINCT(o.uuid)) FILTER (WHERE o.quality_grade == 'research'),0) AS focal_species_rg_count,
        COALESCE(
            ROUND(focal_species_rg_count / NULLIF(focal_species_obs_count,0),4),
            0) AS rg_rate,

        -- N neighbords
        COUNT(DISTINCT(s.similar_taxon_id)) AS similar_species_count,
        CASE
            WHEN similar_species_count> 0 THEN TRUE
            ELSE FALSE
        END AS has_similar_species,

        -- Neigbors counts agg
        SUM(n.similar_species_obs_count) AS nbor_obs_count_sum,
        ROUND(AVG(n.similar_species_obs_count),2) AS nbor_obs_count_mean,
        ROUND(stddev(n.similar_species_obs_count),5) AS nbor_obs_count_std,
        MAX(n.similar_species_obs_count) AS  nbor_obs_count_max,

        -- Neighbors rg agg
        ROUND(AVG(n.similar_species_rg_rate), 2) AS nbor_rg_rate_mean,
        ROUND(stddev(n.similar_species_rg_rate),5) AS nbor_rg_rate_std,
        MIN(n.similar_species_rg_rate) AS nbor_rg_rate_min,
        ROUND( AVG(n.similar_species_rg_rate * d.taxonomic_distance), 3) AS weighted_mean_neighbor_rg_rate,

        -- Neighbors taxo distance
        MAX(d.taxonomic_distance) AS nbor_dist_max,
        ROUND(AVG(d.taxonomic_distance),2) AS nbor_dist_mean,

        -- Focal species rg comparison
        rg_rate - nbor_rg_rate_mean AS rg_rate_vs_neighbors --higher is easier


    FROM staged.similar_species s
    JOIN similar_species_agg n ON s.similar_taxon_id = n.similar_taxon_id
    JOIN features.taxa_distance d ON d.taxon_id = s.taxon_id
    JOIN staged.observations o ON s.taxon_id = o.taxon_id
    GROUP BY s.taxon_id

),
neighborhood_pool AS(

    -- Neighbors, attributed to their focal taxon's pool
    SELECT
        taxon_id AS focal_taxon_id,
        similar_taxon_id           AS pool_member_id,
        similar_species_rg_rate    AS rg_rate,
        FALSE                      AS is_focal
    FROM similar_species_agg

    UNION ALL

    -- Focal taxon injected into its own pool
    SELECT
        t.taxon_id AS focal_taxon_id,
        t.taxon_id AS pool_member_id,
        t.rg_rate,
        TRUE       AS is_focal
    FROM aggregates t
    WHERE t.taxon_id IN (SELECT DISTINCT taxon_id::INT FROM similar_species_agg)

),

ranked AS (
    SELECT
        *,
        PERCENT_RANK() OVER (
            PARTITION BY focal_taxon_id
            ORDER BY rg_rate ASC          -- 0.0 = hardest in pool, 1.0 = easiest
        ) AS rg_percentile_in_neighborhood,
    FROM neighborhood_pool
)

SELECT a.*,

    -- Taxon rg rate ranked in neighbors pool
    r.rg_percentile_in_neighborhood,
    -- Neighbor taxonomic spread
    n.*EXCLUDE(n.taxon_id),

    -- Assymetry
    s.*EXCLUDE(s.taxon_id),

    -- Relative stand

    -- Summary statistic
    ROUND((1- a.weighted_mean_neighbor_rg_rate) * LOG(a.similar_species_count + 1),4) AS neighborhood_difficulty


FROM aggregates a
JOIN nbor_taxa_diversity n ON n.taxon_id = a.taxon_id
JOIN features.taxa_assymetry s ON a.taxon_id = s.taxon_id
JOIN ranked r ON a.taxon_id = r.focal_taxon_id
WHERE r.is_focal = TRUE
