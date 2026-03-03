INSTALL json;
LOAD json;

/home/etienne/.dbclient/query/1772466990294@@127.0.0.1@3306@raw@main/json_unpack.sql.sql
CREATE OR REPLACE TABLE observations AS
SELECT obs.*
FROM (
    SELECT from_json(
        json,
        '{
            "uuid":"VARCHAR",
            "id":"UBIGINT",
            "reviewed_by":["UBIGINT"],
            "owners_identification_from_vision":"NULL",
            "identifications_count":"UBIGINT",
            "user":{
                "id":"UBIGINT",
                "created_at":"VARCHAR",
                "orcid":"NULL"
            },
            "description":"NULL",
            "tags":["NULL"],
            "observation_photos":[{"id":"UBIGINT"}],
            "comments_count":"UBIGINT",
            "faves_count":"UBIGINT",
            "outlinks":["NULL"],
            "community_taxon_id":"NULL",
            "taxon_geoprivacy":"NULL",
            "place_ids":["UBIGINT"],
            "identifications":[{
                "id":"UBIGINT",
                "uuid":"VARCHAR",
                "created_at":"VARCHAR",
                "user":{
                    "id":"UBIGINT",
                    "login":"VARCHAR",
                    "observations_count":"UBIGINT",
                    "identifications_count":"UBIGINT",
                    "species_count":"UBIGINT"
                },
                "body":"VARCHAR",
                "category":"VARCHAR",
                "current":"BOOLEAN",
                "own_observation":"BOOLEAN",
                "vision":"BOOLEAN",
                "disagreement":"NULL",
                "previous_observation_taxon_id":"NULL",
                "taxon_id":"UBIGINT"
            }]
        }'
    ) AS obs
    FROM ina_api
) t;

ALTER TABLE observations
ALTER COLUMN user.created_at TYPE TIMESTAMP
USING strptime(user.created_at, '%Y-%m-%dT%H:%M:%S%z');


