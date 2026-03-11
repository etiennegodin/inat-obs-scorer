CREATE SCHEMA IF NOT EXISTS staged;

CREATE OR REPLACE TABLE staged.similar_species AS

WITH unpacked AS (
    SELECT
    taxon_id::INT AS taxon_id,
    data."count" AS "count",
    UNNEST(data.taxon, RECURSIVE := true)

    FROM (
        SELECT raw_id AS taxon_id,
        from_json(
            raw_json,
            '{
                "taxon":{
                    "id":"UBIGINT",
                },
                "count":"UBIGINT",
            }'
            ) AS data
            FROM raw.api_similar_species
        )
)

SELECT * EXCLUDE('id'),

id AS similar_taxon_id
FROM unpacked
LIMIT 100
;
