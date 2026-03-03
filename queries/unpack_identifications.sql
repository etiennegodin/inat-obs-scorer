CREATE OR REPLACE TABLE identifications AS
SELECT 
    o.id AS observation_id,
    UNNEST(o.identifications, RECURSIVE := true)
FROM observations o;
