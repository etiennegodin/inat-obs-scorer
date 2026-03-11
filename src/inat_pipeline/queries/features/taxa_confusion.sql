CREATE OR REPLACE TABLE features.taxa_confusion AS


WITH nbor_obs_counts AS(

SELECT
s.similar_taxon_id,

COUNT(DISTINCT(o.uuid)) AS similar_species_obs_count

FROM staged.similar_species s
LEFT JOIN staged.observations o on o.taxon_id = s.similar_taxon_id
GROUP BY s.similar_taxon_id
),

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
-- Neigbors counts aggregates
SUM(n.similar_species_obs_count) AS nbor_obs_count_sum,
ROUND(AVG(n.similar_species_obs_count),2) AS nbor_obs_count_mean,
MAX(n.similar_species_obs_count) AS  nbor_obs_count_max,


-- Neighbors taxo distance
MAX(d.taxonomic_distance) AS nbor_dist_max,
ROUND(AVG(d.taxonomic_distance),2) AS nbor_dist_mean,


FROM staged.similar_species s
JOIN nbor_obs_counts n on s.similar_taxon_id = n.similar_taxon_id
JOIN features.taxa_distance d ON d.taxon_id = s.taxon_id
GROUP BY s.taxon_id

)


SELECT a.*,

-- Neighbor diversity
n.*EXCLUDE(n.taxon_id),

-- Assymetry
s.*EXCLUDE(s.taxon_id),

FROM aggregates a
JOIN nbor_taxa_diversity n ON n.taxon_id = a.taxon_id
JOIN features.taxa_assymetry s ON a.taxon_id = s.taxon_id
