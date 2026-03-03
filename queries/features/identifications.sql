CREATE OR REPLACE TABLE features.identifications AS


SELECT i.observation_id,

COUNT(DISTINCT i.id) as id_count_total,
COUNT(DISTINCT i.id) FILTER (WHERE i.own_observation IS FALSE) as id_count_community,
COUNT(DISTINCT i.id) FILTER (WHERE i.category = 'supporting') as id_count_agreeing,
COUNT(DISTINCT i.id) FILTER (WHERE i.category = 'improving') as id_count_improving,
COUNT(DISTINCT i.id) FILTER (WHERE i.category = 'maverick') as id_count_maverick,
COUNT(DISTINCT i.id) FILTER (WHERE i.vision IS TRUE) as id_count_vision_used,
MIN(i.created_at) FILTER (WHERE i.own_observation IS FALSE) - o.created_at as time_to_first_community_id_hours 

FROM staged.identifications i
JOIN staged.observations o ON i.observation_id = o.id
GROUP BY i.observation_id, o.created_at