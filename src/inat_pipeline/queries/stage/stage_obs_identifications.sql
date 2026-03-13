-- ids --

CREATE OR REPLACE TABLE staged.identifications AS
SELECT
    o.id AS observation_id,
    UNNEST(o.identifications, RECURSIVE := true),

FROM staged.observations o;

CREATE OR REPLACE TABLE staged.identifications AS
SELECT i.*,
    t.phylum,
    t.class,
    t."order",
    t.family,
    t.genus,
    t.species,
    t.rank

FROM staged.identifications i
JOIN staged.taxa t ON i.taxon_id = t.taxon_id;


ALTER TABLE staged.identifications
RENAME id_1 TO user_id;

ALTER TABLE staged.identifications
RENAME login TO user_login;

ALTER TABLE staged.identifications
RENAME observations_count TO user_observations_count;

ALTER TABLE staged.identifications
RENAME identifications_count TO user_identifications_count;

ALTER TABLE staged.identifications
RENAME species_count TO user_species_count;

ALTER TABLE staged.identifications
ALTER COLUMN created_at
SET DATA TYPE TIMESTAMPTZ
USING COALESCE(
    try_strptime(created_at, '%Y-%m-%d %H:%M:%S %z'), -- Handles -0400
    try_strptime(created_at, '%Y-%m-%d %H:%M:%S %Z'), -- Handles UTC
    try_cast(created_at AS TIMESTAMPTZ)               -- Fallback to default
);

CREATE OR REPLACE TABLE staged.identifications AS
SELECT * FROM staged.identifications
ORDER BY observation_id, created_at;
