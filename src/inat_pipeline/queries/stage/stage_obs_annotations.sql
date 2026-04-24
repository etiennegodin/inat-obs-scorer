-- annotations --

CREATE OR REPLACE TABLE staged.annotations AS
SELECT
    o.id AS observation_id,
    UNNEST(o.annotations, RECURSIVE := true)
FROM staged.observations o;
