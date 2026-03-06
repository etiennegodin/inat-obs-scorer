-- photos -- 

CREATE OR REPLACE TABLE staged.photos AS
SELECT 
    o.id AS observation_id,
    UNNEST(o.observation_photos, RECURSIVE := true)
FROM staged.observations o;
ALTER TABLE staged.photos
RENAME id TO photo_id;

CREATE OR REPLACE TABLE staged.observers AS
SELECT 
    UNNEST(o.user)
FROM staged.observations o;

CREATE OR REPLACE TABLE staged.observers AS
SELECT DISTINCT(u.id) AS user_id,
u.* EXCLUDE(id),


FROM staged.observers u;

ALTER TABLE staged.observers
ALTER COLUMN created_at 
SET DATA TYPE TIMESTAMPTZ 
USING COALESCE(
    try_strptime(created_at, '%Y-%m-%d %H:%M:%S %z'), -- Handles -0400
    try_strptime(created_at, '%Y-%m-%d %H:%M:%S %Z'), -- Handles UTC
    try_cast(created_at AS TIMESTAMPTZ)               -- Fallback to default
);

ALTER TABLE staged.observers
ADD COLUMN observer BOOLEAN DEFAULT TRUE;

ALTER TABLE staged.observers
ADD COLUMN identifier BOOLEAN DEFAULT NULL;



CREATE OR REPLACE TABLE staged.identifiers AS
SELECT DISTINCT(user_id)
FROM staged.identifications;

ALTER TABLE staged.identifiers
ADD COLUMN created_at TIMESTAMPTZ DEFAULT NULL;

ALTER TABLE staged.identifiers
ADD COLUMN orcid INTEGER DEFAULT NULL;


ALTER TABLE staged.identifiers
ADD COLUMN identifier BOOLEAN DEFAULT TRUE;
ALTER TABLE staged.identifiers
ADD COLUMN observer BOOLEAN DEFAULT NULL;

CREATE OR REPLACE TABLE staged.users AS
SELECT user_id, created_at, orcid, observer, identifier FROM staged.observers
UNION ALL
SELECT user_id, created_at, orcid, observer, identifier FROM staged.identifiers;


CREATE OR REPLACE TABLE staged.users AS



