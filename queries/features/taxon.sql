CREATE OR REPLACE TABLE features.taxon AS

WITH aggregates AS(

    SELECT o.id,
    o.taxon_id,
    COALESCE(
        COUNT(*) OVER taxon_history, 0
        )AS taxon_obs_count,

    -- Baseline rates
    COALESCE(
        SUM(DISTINCT(o.id)) FILTER (
            CASE WHEN quality_grade = 'research' THEN 1.0 ELSE 0 END
            ) OVER taxon_history / taxon_obs_count, 0
        ) AS taxon_rg_rate,

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



