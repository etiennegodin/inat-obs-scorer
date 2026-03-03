-- users -- 
CREATE OR REPLACE TABLE users AS
SELECT 
    UNNEST(o.user, RECURSIVE := true)
FROM observations o;

-- identifications --
CREATE OR REPLACE TABLE identifications AS
SELECT 
    o.id AS observation_id,
    UNNEST(o.identifications, RECURSIVE := true)
FROM observations o;

-- photos -- 
CREATE OR REPLACE TABLE photos AS
SELECT 
    o.id AS observation_id,
    UNNEST(o.observation_photos, RECURSIVE := true)
FROM observations o;

