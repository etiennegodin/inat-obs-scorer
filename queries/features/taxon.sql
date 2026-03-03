CREATE OR REPLACE TABLE features.taxon AS

WITH aggregates AS(

SELECT o.taxon_id,
COUNT(DISTINCT(o.id)) as taxon_obs_count,

-- Baseline rates
COUNT(DISTINCT(o.id)) FILTER (WHERE o.quality_grade = 'research') / taxon_obs_count as taxon_rg_rate,
LOG(taxon_obs_count) AS taxon_popularity_rank,

    -- Difficulty flags
--    is_difficult_group          BOOLEAN,    -- fungi, lichens, bryophytes, micro-invertebrates

-- ID convergence tendency
AVG(LENGTH(o.identifications)) FILTER (WHERE o.quality_grade = 'research' ) AS taxon_avg_ids_to_rg,
--    taxon_pct_needs_community   FLOAT       -- pct that require >1 identifier

FROM staged.observations o
LEFT JOIN features.identifications i ON i.observation_id = o.id
GROUP BY o.taxon_id

)

SELECT a.*,
t.phylum,
t.class,
t."order",
t.family,
t.genus,
t.species,
t."taxonRank"



FROM aggregates a 
LEFT JOIN staged.taxa t on a.taxon_id = t.id
