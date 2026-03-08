CREATE OR REPLACE MACRO research_grade(eval_interval) AS TABLE

SELECT 
    o.id AS observation_id,

    COUNT(DISTINCT(i.user_id)) FILTER (
        WHERE i.own_observation IS FALSE
        AND i.created_at <= o.created_at + eval_interval
        ) AS n_identifiers,
    n_identifiers > 0 AS label

FROM staged.observations o 
JOIN staged.identifications i ON o.id = i.observation_id
JOIN community_taxon_windowed(eval_interval) t ON o.id = t.observation_id

WHERE o.created_at IS NOT NULL
AND o.latitude IS NOT NULL
AND LENGTH(o.observation_photos) > 0
AND o.captive_cultivated is FALSE
AND t.community_taxon = o.taxon_id
AND t.consensus_level_rg IS TRUE

GROUP BY o.id;



