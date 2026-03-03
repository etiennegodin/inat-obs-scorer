CREATE OR REPLACE TABLE features.identifiers AS

SELECT i.user_id,

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
COUNT(DISTINCT(i.order_id)) FILTER (WHERE i.order_id > 0) as taxon_diversity_order,
COUNT(DISTINCT(i.family_id)) FILTER (WHERE i.family_id > 0) as taxon_diversity_family,
COUNT(DISTINCT(i.genus_id)) FILTER (WHERE i.genus_id > 0) as taxon_diversity_genus,
COUNT(DISTINCT(i.species_id)) FILTER (WHERE i.species_id > 0) as taxon_diversity_species,

FROM staged.identifications i
GROUP BY i.user_id