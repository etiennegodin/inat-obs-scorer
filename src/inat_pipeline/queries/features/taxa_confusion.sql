CREATE OR REPLACE TABLE features.taxa_confusion AS

--Neighbors count
WITH nbor_obs_counts AS(
    SELECT
        s.similar_taxon_id,

        COUNT(DISTINCT(o.uuid))AS similar_species_obs_count,
        COALESCE(COUNT(DISTINCT(o.uuid)) FILTER (WHERE o.quality_grade == 'research'),0) AS similar_species_rg_count,
        COALESCE(
            ROUND(similar_species_rg_count / NULLIF(similar_species_obs_count,0),4),
            0) AS similar_species_rg_rate

    FROM staged.similar_species s
    LEFT JOIN staged.observations o on o.taxon_id = s.similar_taxon_id
    WHERE s.taxon_id = 50717
    GROUP BY s.similar_taxon_id
),
--Neighbor taxonomic spread
nbor_taxa_diversity AS(
    SELECT s.taxon_id,
        COUNT(DISTINCT(t.genus_id)) AS neighbor_genus_diversity,
        MIN(t.rank_level) neighbor_rank_min

    FROM staged.similar_species s
    JOIN staged.taxa t on s.taxon_id = t.taxon_id
    GROUP BY s.taxon_id
),


aggregates AS(

    SELECT s.taxon_id::INT AS taxon_id,
        -- N neighbords
        COUNT(DISTINCT(s.similar_taxon_id)) AS similar_species_count,
        CASE
            WHEN similar_species_count> 0 THEN TRUE
            ELSE FALSE
        END AS has_similar_species,
        -- Neigbors counts agg
        SUM(n.similar_species_obs_count) AS nbor_obs_count_sum,
        ROUND(AVG(n.similar_species_obs_count),2) AS nbor_obs_count_mean,
        MAX(n.similar_species_obs_count) AS  nbor_obs_count_max,

        -- Neighbors rg agg
        ROUND(AVG(n.similar_species_rg_rate), 2) AS nbor_rg_rate_mean,
        MIN(n.similar_species_rg_rate) AS nbor_rg_rate_min,
        ROUND( AVG(n.similar_species_rg_rate * d.taxonomic_distance), 3) AS weighted_mean_neighbor_rg_rate,

        -- Neighbors taxo distance
        MAX(d.taxonomic_distance) AS nbor_dist_max,
        ROUND(AVG(d.taxonomic_distance),2) AS nbor_dist_mean,


    FROM staged.similar_species s
    JOIN nbor_obs_counts n on s.similar_taxon_id = n.similar_taxon_id
    JOIN features.taxa_distance d ON d.taxon_id = s.taxon_id
    GROUP BY s.taxon_id

)

SELECT a.*,

    -- Neighbor taxonomic spread
    n.*EXCLUDE(n.taxon_id),

    -- Assymetry
    s.*EXCLUDE(s.taxon_id),

    -- Relative stand

    -- Summary statistic
    ROUND((1- a.weighted_mean_neighbor_rg_rate) * LOG(a.similar_species_count + 1),4) AS neighborhood_difficulty


FROM
FROM aggregates a
JOIN nbor_taxa_diversity n ON n.taxon_id = a.taxon_id
JOIN features.taxa_assymetry s ON a.taxon_id = s.taxon_id
