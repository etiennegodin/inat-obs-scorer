-- OBSERVERS --

-- unpack
CREATE OR REPLACE TABLE staged.observers AS
SELECT
    UNNEST(o.user)
FROM staged.observations o;

-- distinct only
CREATE OR REPLACE TABLE staged.observers AS
SELECT DISTINCT(u.id) AS user_id,
u.* EXCLUDE(id),
FROM staged.observers u;

-- handle timestamps

ALTER TABLE staged.observers
ALTER COLUMN created_at
SET DATA TYPE TIMESTAMPTZ
USING COALESCE(
    try_strptime(created_at, '%Y-%m-%d %H:%M:%S %z'), -- Handles -0400
    try_strptime(created_at, '%Y-%m-%d %H:%M:%S %Z'), -- Handles UTC
    try_cast(created_at AS TIMESTAMPTZ)               -- Fallback to default
);

-- add observer and identifier columns
ALTER TABLE staged.observers
ADD COLUMN observer BOOLEAN DEFAULT TRUE;
ALTER TABLE staged.observers
ADD COLUMN identifier BOOLEAN DEFAULT NULL;


-- OBSERVERS --

-- unpack
CREATE OR REPLACE TABLE staged.identifiers AS
SELECT DISTINCT(user_id)
FROM staged.identifications;

-- add empty metadata columns
ALTER TABLE staged.identifiers
ADD COLUMN created_at TIMESTAMPTZ DEFAULT NULL;
ALTER TABLE staged.identifiers
ADD COLUMN orcid INTEGER DEFAULT FALSE;

-- add observer and identifier columns

ALTER TABLE staged.identifiers
ADD COLUMN identifier BOOLEAN DEFAULT TRUE;
ALTER TABLE staged.identifiers
ADD COLUMN observer BOOLEAN DEFAULT FALSE;

-- MERGE

CREATE OR REPLACE TABLE staged.users AS
SELECT user_id, created_at, orcid, observer, identifier FROM staged.observers
UNION ALL
SELECT user_id, created_at, orcid, observer, identifier FROM staged.identifiers;

-- Aggregate all
CREATE OR REPLACE TABLE staged.users AS

WITH agg AS(
    SELECT DISTINCT(u.user_id) AS user_id,
    MAX(created_at) as created_at,
    MAX(orcid) as orcid,
    MAX(observer) as observer,
    MAX(identifier) as identifier,
    FROM staged.users u
    GROUP BY user_id
)

SELECT
a.*,
CASE
    WHEN a.observer IS TRUE AND a.identifier IS FALSE THEN TRUE
    ELSE FALSE
END AS observer_only,
CASE
    WHEN a.identifier IS TRUE AND a.observer IS FALSE THEN TRUE
    ELSE FALSE
END AS identifier_only
FROM agg a
