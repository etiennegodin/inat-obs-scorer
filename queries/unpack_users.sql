CREATE OR REPLACE TABLE users AS
SELECT 
    UNNEST(o.user, RECURSIVE := true)
FROM observations o;
