-- ids --

CREATE OR REPLACE TABLE staged.identifications AS
SELECT 
    o.id AS observation_id,
    UNNEST(o.identifications, RECURSIVE := true)
FROM staged.observations o;

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

ALTER TABLE identifications 
ALTER COLUMN created_at 
SET DATA TYPE TIMESTAMPTZ 
USING COALESCE(
    try_strptime(created_at, '%Y-%m-%d %H:%M:%S %z'), -- Handles -0400
    try_strptime(created_at, '%Y-%m-%d %H:%M:%S %Z'), -- Handles UTC
    try_cast(created_at AS TIMESTAMPTZ)               -- Fallback to default
);

-- photos -- 

CREATE OR REPLACE TABLE staged.photos AS
SELECT 
    o.id AS observation_id,
    UNNEST(o.observation_photos, RECURSIVE := true)
FROM staged.observations o;
ALTER TABLE staged.photos
RENAME id TO photo_id;

CREATE OR REPLACE TABLE staged.users AS
SELECT 
    UNNEST(o.user)
FROM staged.observations o;

ALTER TABLE staged.users
RENAME id TO user_id;