CREATE OR REPLACE TABLE features.identifications AS


SELECT i.observation_id,

COUNT(DISTINCT i.id) AS id_count_total,
COUNT(DISTINCT i.id) FILTER (WHERE i.own_observation IS FALSE) AS id_count_community,
COUNT(DISTINCT i.id) FILTER (WHERE i.category = 'supporting') AS id_count_agreeing,
COUNT(DISTINCT i.id) FILTER (WHERE i.category = 'improving') AS id_count_improving,
COUNT(DISTINCT i.id) FILTER (WHERE i.category = 'maverick') AS id_count_maverick,
COUNT(DISTINCT i.id) FILTER (WHERE i.vision IS TRUE) AS id_count_vision_used,
MIN(i.created_at) FILTER (WHERE i.own_observation IS FALSE) - o.created_at AS time_to_first_community_id_hours 

FROM staged.identifications i
JOIN staged.observations o ON i.observation_id = o.id
GROUP BY i.observation_id, o.created_at