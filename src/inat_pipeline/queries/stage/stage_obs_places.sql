-- places --
CREATE OR REPLACE TABLE staged.places AS
WITH unpacked AS (
SELECT
    UNNEST(o.place_ids, RECURSIVE := true) AS place_id
    FROM staged.observations o
),

uniques AS(

    SELECT DISTINCT(place_id)
    FROM unpacked
)
SELECT u.*,
p.*EXCLUDE(id)
FROM uniques u
JOIN raw.places p ON u.place_id = p.id
