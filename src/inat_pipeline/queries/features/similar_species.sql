CREATE OR REPLACE TABLE features.taxa_confusion AS


WITH nbor_obs_counts AS(

SELECT
s.similar_taxon_id,

COUNT(DISTINCT(o.uuid)) AS similar_species_obs_count

FROM staged.similar_species s
LEFT JOIN staged.observations o on o.taxon_id = s.similar_taxon_id
GROUP BY s.similar_taxon_id
),

direction AS(

    SELECT s.taxon_id,



    FROM staged.similar_species s
    GROUP BY s.taxon_id
)

SELECT s.taxon_id,

COUNT(DISTINCT(s.similar_taxon_id)) as similar_species_count,
SUM(n.similar_species_obs_count) nbor_obs_count_sum,
AVG(n.similar_species_obs_count) nbor_obs_count_mean,
MAX(n.similar_species_obs_count) nbor_obs_count_max

FROM staged.similar_species s
JOIN nbor_obs_counts n on s.similar_taxon_id = n.similar_taxon_id
GROUP BY s.taxon_id
