
WITH community_taxon AS
(
    SELECT id,


    FROM staged.identifications

)

SELECT o.id

COUNT(DISTINCT(i.user_id)) as n_identifiers


FROM staged.observations o 
JOIN staged.identifications i ON o.id = i.observation_id
GROUP BY o.id

WHERE o.created_at IS NOT NULL
AND o.latitude IS NOT NULL
AND LENGTH(o.observation_photos) > 0
AND o.captive_cultivated is FALSE


