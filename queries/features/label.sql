CREATE OR REPLACE TABLE features.label AS

SELECT o.id as observation_id,

COUNT(DISTINCT(i.user_id)) FILTER (WHERE i.own_observation IS FALSE) AS n_identifiers,

CASE 
    WHEN n_identifiers > 0 THEN TRUE
    ELSE FALSE
END AS label  


FROM staged.observations o 
JOIN staged.identifications i ON o.id = i.observation_id
JOIN features.community_taxon t ON o.id = t.observation_id

WHERE o.created_at IS NOT NULL
AND o.latitude IS NOT NULL
AND LENGTH(o.observation_photos) > 0
AND o.captive_cultivated is FALSE
AND t.community_taxon = o.taxon_id
AND t.consensus_level_rg IS TRUE
--added time filter
AND o.created_at - i.created_at < INTERVAL '90 days'

GROUP BY o.id;



