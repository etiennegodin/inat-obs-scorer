CREATE OR REPLACE MACRO research_grade_windowed(eval_interval) AS TABLE

SELECT 
    o.id AS observation_id,
    t.community_taxon AS taxon_id,
    o.taxon_id as tx,
    t.consensus_level,
    t.consensus_level_rg,
    o.created_at,

    -- n identifiers
    COUNT(DISTINCT(i.user_id)) FILTER (
        WHERE i.own_observation IS FALSE
        AND i.created_at <= o.created_at + eval_interval
        ) AS n_identifiers_at_window,
    -- n ids 
    COUNT(i.id) FILTER (
        WHERE i.created_at BETWEEN o.created_at AND o.created_at + eval_interval
        AND i.own_observation IS FALSE
    ) AS n_ids_at_window,

    -- research grade label 
    CASE 
        WHEN o.created_at IS NULL                  THEN 'ineligible'
        WHEN o.latitude IS NULL                    THEN 'ineligible'
        WHEN LENGTH(o.observation_photos) = 0      THEN 'ineligible'
        WHEN o.captive_cultivated IS TRUE          THEN 'ineligible'

        -- Community taxon doesn't match submitted taxon
        WHEN t.community_taxon != o.taxon_id       THEN 'ineligible'

        -- Eligible but consensus didn't reach species level
        WHEN t.consensus_level_rg IS FALSE         THEN 'not_rg'
        -- Eligible, species consensus, but no confirming identifiers
        WHEN n_ids_at_window = 0                   THEN 'not_rg'
        ELSE 'research_grade'
    END AS rg_label,
    
    rg_label = 'research_grade' AS is_rg




FROM staged.observations o 
JOIN staged.identifications i ON o.id = i.observation_id
JOIN community_taxon_windowed(eval_interval) t ON o.id = t.observation_id


WHERE o.created_at IS NOT NULL  
GROUP BY o.id,
    t.community_taxon,
    t.consensus_level,
    t.consensus_level_rg,
    o.taxon_id,
    o.created_at,
    o.latitude,
    o.observation_photos,
    o.captive_cultivated,

;



