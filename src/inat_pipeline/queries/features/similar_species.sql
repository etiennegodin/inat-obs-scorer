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

SELECT s.taxon_id,

COUNT(DISTINCT(s.similar_taxon_id)) as similar_species_count,
SUM(n.similar_species_obs_count) nbor_obs_count_sum,
ROUND(AVG(n.similar_species_obs_count),2) nbor_obs_count_mean,
MAX(n.similar_species_obs_count) nbor_obs_count_max,


FROM staged.similar_species s
JOIN nbor_obs_counts n on s.similar_taxon_id = n.similar_taxon_id
JOIN features.taxa_assymetry a ON s.taxon_id = a.taxon_id
GROUP BY s.taxon_id

)


SELECT a.*,
s.out_degree,
s.in_degree,
s.magnet_score,
n.neighbor_genus_diversity,
n.neighbor_rank_min
FROM aggregates a, nbor_taxa_diversity n
JOIN features.taxa_assymetry s ON a.taxon_id = s.taxon_id
