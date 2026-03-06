CREATE OR REPLACE TABLE features.identifiers AS

SELECT i.observation_id
i.user_id,

-- Volume 
COUNT(DISTINCT(i.id)) AS total_id_count,
COUNT(DISTINCT(i.id)) FILTER (WHERE i.own_observation IS FALSE) AS ids_given_count,
CASE WHEN ids_given_count > 0 THEN TRUE ELSE FALSE END AS gave_id,



-- Community 
COUNT(DISTINCT(i.id)) FILTER (
    WHERE i.category = 'supporting'
    ) / ids_given_count AS ids_given_agreeing_pct,

-- Ai vision
COUNT(DISTINCT(i.id)) FILTER (
    WHERE i.vision = TRUE
    ) / ids_given_count AS ids_given_vision_pct,


-- Taxonomic behaviour
COUNT(DISTINCT(i.order)) FILTER (WHERE i.order IS NOT NULL) AS taxon_diversity_order,
COUNT(DISTINCT(i.family)) FILTER (WHERE i.family IS NOT NULL) AS taxon_diversity_family,
COUNT(DISTINCT(i.genus)) FILTER (WHERE i.genus IS NOT NULL) AS taxon_diversity_genus,
COUNT(DISTINCT(i.species)) FILTER (WHERE i.species IS NOT NULL) AS taxon_diversity_species,

FROM staged.identifications i
GROUP BY i.user_id