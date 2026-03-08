CREATE OR REPLACE TABLE features.taxon AS

WITH aggregates AS(

    SELECT 
    rg.observation_id,
    rg.taxon_id,
    rg.created_at,

    COALESCE(
        COUNT(*) OVER taxon_history, 0
        )AS taxon_obs_count,

    COALESCE(COUNT(*) FILTER (WHERE rg.is_rg)  OVER taxon_history, 0) AS taxon_rg_obs,

    -- Baseline rates
    taxon_rg_obs::FLOAT / NULLIF(taxon_obs_count, 0) AS taxon_rg_rate,
    COALESCE(taxon_rg_rate, 0)                        AS taxon_rg_rate_safe,

    LOG(taxon_obs_count + 1) AS taxon_popularity_rank,

        -- Difficulty flags
    --    is_difficult_group          BOOLEAN,    -- fungi, lichens, bryophytes, micro-invertebrates

    -- ID convergence tendency
    COALESCE(
        AVG(rg.n_ids_at_window) FILTER (
            WHERE rg.is_rg
            ) OVER taxon_history, 0
    ) AS taxon_avg_ids_to_rg,
    --    taxon_pct_needs_community   FLOAT       -- pct that require >1 identifier

    FROM research_grade_windowed(INTERVAL '999 years') rg

    WINDOW
        taxon_history AS (
            PARTITION BY taxon_id
            ORDER BY created_at
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



