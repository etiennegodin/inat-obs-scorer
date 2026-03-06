CREATE OR REPLACE TABLE features.taxon AS

WITH aggregates AS(

    SELECT o.id AS observation_id,
    o.taxon_id,
    COALESCE(
        COUNT(*) OVER taxon_history, 0
        )AS taxon_obs_count,

    -- Baseline rates
    COALESCE(
        COUNT(*) FILTER (
            WHERE o.quality_grade = 'research'
            ) OVER taxon_history, 0
        ) AS taxon_rg_obs,
    
    taxon_rg_obs / taxon_obs_count AS taxon_rg_rate_raw,
    CASE WHEN isnan(taxon_rg_rate_raw) THEN 0 ELSE taxon_rg_rate_raw END AS taxon_rg_rate,

    LOG(taxon_obs_count + 1) AS taxon_popularity_rank,

        -- Difficulty flags
    --    is_difficult_group          BOOLEAN,    -- fungi, lichens, bryophytes, micro-invertebrates

    -- ID convergence tendency
    COALESCE(
        AVG(LENGTH(o.identifications)) FILTER (
            WHERE o.quality_grade = 'research'
            ) OVER taxon_history, 0
    ) AS taxon_avg_ids_to_rg,
    --    taxon_pct_needs_community   FLOAT       -- pct that require >1 identifier

    FROM staged.observations o

    WINDOW
        taxon_history AS (
            PARTITION BY o.taxon_id
            ORDER BY o.created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )

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



