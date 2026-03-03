CREATE OR REPLACE TABLE staged.identifications AS
SELECT 
    o.id AS observation_id,
    UNNEST(o.identifications, RECURSIVE := true)
FROM observations o;

ALTER TABLE staged.identifications 
RENAME id_1 TO user_id;

ALTER TABLE staged.identifications 
RENAME login TO user_login;

ALTER TABLE staged.identifications 
RENAME observations_count TO user_observations_count;

ALTER TABLE staged.identifications 
RENAME identifications_count TO user_identifications_count;

ALTER TABLE staged.identifications 
RENAME species_count TO user_species_count

CREATE OR REPLACE TABLE staged.photos AS
SELECT 
    o.id AS observation_id,
    UNNEST(o.observation_photos, RECURSIVE := true) as photo_id
FROM observations o;
ALTER TABLE staged.photos
RENAME id TO photo_id;

CREATE OR REPLACE TABLE staged.users AS
SELECT 
    UNNEST(o.user)
FROM observations o;

ALTER TABLE staged.users
RENAME id TO user_id;