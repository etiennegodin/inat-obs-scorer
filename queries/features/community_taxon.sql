CREATE OR REPLACE VIEW features.community_taxon AS

SELECT i.observation_id,
COUNT(*) AS total_ids,

-- How many distinct values exist at each rank (disagreement signal)
COUNT(DISTINCT i.class)     AS class_distinct_count,
COUNT(DISTINCT i."order")   AS order_distinct_count,
COUNT(DISTINCT i.family)    AS family_distinct_count,
COUNT(DISTINCT i.genus)     AS genus_distinct_count,
COUNT(DISTINCT i.species)   AS species_distinct_count,


COUNT(i.class)     AS class_count,
COUNT(i."order")   AS order_count,
COUNT(i.family)    AS family_count,
COUNT(i.genus)     AS genus_count,
COUNT(i.species)   AS specie_count,

-- Full distribution: {value: count, ...} per rank
HISTOGRAM(i.class)          AS class_map,
HISTOGRAM(i."order")        AS order_map,
HISTOGRAM(i.family)         AS family_map,
HISTOGRAM(i.genus)          AS genus_map,
HISTOGRAM(i.species)        AS species_map,


list_max(map_values(class_map))::FLOAT / class_count::FLOAT AS class_top_proportion,
list_max(map_values(order_map))::FLOAT / order_count::FLOAT AS order_top_proportion,
list_max(map_values(family_map))::FLOAT / family_count::FLOAT AS family_top_proportion,
list_max(map_values(genus_map))::FLOAT / genus_count::FLOAT AS genus_top_proportion,
list_max(map_values(species_map))::FLOAT / specie_count::FLOAT AS species_top_proportion,

CASE
    WHEN species_top_proportion >= (2/3) THEN 'species'
    WHEN genus_top_proportion   >= (2/3)  THEN 'genus'
    WHEN family_top_proportion  >= (2/3) THEN 'family'
    WHEN "order_top_proportion" >= (2/3) THEN 'order'
    ELSE 'class'
END AS consensus_level,

CASE consensus_level
    WHEN 'species' THEN species_map
    WHEN 'genus'   THEN genus_map
    WHEN 'family'  THEN family_map
    WHEN 'order'   THEN order_map
    ELSE class_map
END AS consensus_level_histogram,
unnest(map_keys(consensus_level_histogram))                                          AS community_taxon,




FROM staged.identifications i
WHERE i."current" IS TRUE
GROUP BY i.observation_id;
