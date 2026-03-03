CREATE OR REPLACE TABLE photos AS
SELECT 
    o.id AS observation_id,
    UNNEST(o.observation_photos, RECURSIVE := true)
FROM observations o;
