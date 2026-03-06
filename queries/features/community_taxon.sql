CREATE OR REPLACE VIEW features.community_taxon AS

SELECT i.observation_id,
COUNT(*) AS total_ids,

-- How many distinct values exist at each rank (disagreement signal)
COUNT(DISTINCT i.class)     AS class_distinct_count,
COUNT(DISTINCT i."order")   AS order_distinct_count,
COUNT(DISTINCT i.family)    AS family_distinct_count,
COUNT(DISTINCT i.genus)     AS genus_distinct_count,
COUNT(DISTINCT i.species)   AS species_distinct_count,

-- Full distribution: {value: count, ...} per rank
HISTOGRAM(i.class)          AS class_counts,
HISTOGRAM(i."order")        AS order_counts,
HISTOGRAM(i.family)         AS family_counts,
HISTOGRAM(i.genus)          AS genus_counts,
HISTOGRAM(i.species)        AS species_counts


FROM staged.identifications i
GROUP BY i.observation_id