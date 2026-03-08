-- photos -- 

CREATE OR REPLACE TABLE staged.photos AS
SELECT 
    o.id AS observation_id,
    UNNEST(o.observation_photos, RECURSIVE := true)
FROM staged.observations o;
ALTER TABLE staged.photos
RENAME id TO photo_id;
