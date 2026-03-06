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

-- Full distribution with taxon_id
HISTOGRAM(CASE WHEN i."taxonRank" = 'class' THEN i.taxon_id END) AS class_id_map,
HISTOGRAM(CASE WHEN i."taxonRank" = 'order' THEN i.taxon_id END) AS order_id_map,
HISTOGRAM(CASE WHEN i."taxonRank" = 'family' THEN i.taxon_id END) AS family_id_map,
HISTOGRAM(CASE WHEN i."taxonRank" = 'genus' THEN i.taxon_id END) AS genus_id_map,
HISTOGRAM(CASE WHEN i."taxonRank" = 'species' THEN i.taxon_id END) AS species_id_map,


list_max(map_values(class_map))::FLOAT / class_count::FLOAT AS class_top_proportion,
list_max(map_values(order_map))::FLOAT / order_count::FLOAT AS order_top_proportion,
list_max(map_values(family_map))::FLOAT / family_count::FLOAT AS family_top_proportion,
list_max(map_values(genus_map))::FLOAT / genus_count::FLOAT AS genus_top_proportion,
list_max(map_values(species_map))::FLOAT / specie_count::FLOAT AS species_top_proportion,


-- Option A: stop where distinct values = 1 (full agreement)
CASE
    WHEN species_top_proportion >= (2/3) THEN 'species'
    WHEN genus_top_proportion   >= (2/3)  THEN 'genus'
    WHEN family_top_proportion  >= (2/3) THEN 'family'
    WHEN "order_top_proportion" >= (2/3) THEN 'order'
    ELSE 'class'
END AS consensus_level_old,

CASE
    WHEN specie_count = genus_count  THEN 'species'
    WHEN genus_count   = family_count THEN 'genus'
    WHEN family_count  = order_count  THEN 'family'
    ELSE 'order'
END AS consensus_level,


CASE consensus_level
    WHEN 'species' THEN species_id_map
    WHEN 'genus'   THEN genus_id_map
    WHEN 'family'  THEN family_id_map
    WHEN 'order'   THEN order_id_map
    ELSE class_id_map
END AS consensus_level_histogram,

CASE consensus_level
    WHEN 'species' THEN TRUE
    ELSE FALSE
END AS consensus_level_rg,
unnest(map_keys(consensus_level_histogram))                                          AS community_taxon,

FROM staged.identifications i
JOIN staged.observations o ON i.observation_id = o.id

-- non-deleted identfications
WHERE i."current" IS TRUE
-- time filter
AND o.created_at - i.created_at < INTERVAL '90 days'

GROUP BY i.observation_id;
