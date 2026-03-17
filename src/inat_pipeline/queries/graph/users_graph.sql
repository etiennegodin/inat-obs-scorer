CREATE SCHEMA IF NOT EXISTS graph;
-- static

CREATE OR REPLACE TABLE graph.observer_identifier_edges AS

WITH base AS(

SELECT
    o.user_id          AS observer_id,
    i.user_id          AS identifier_id,
    COUNT(*)           AS n_ids,            -- times identifier helped this observer
    COUNT(DISTINCT o.id) AS n_obs_touched  -- unique obs touched
FROM staged.observations o
JOIN staged.identifications i
    ON i.observation_id = o.id
WHERE i.user_id != o.user_id  -- exclude self-identifications
GROUP BY o.user_id, i.user_id
),

identifier_totals AS (
    -- total IDs each identifier has ever left (for normalization)
    SELECT user_id AS identifier_id, COUNT(*) AS identifier_total_ids
    FROM staged.identifications
    GROUP BY user_id
)

SELECT
    b.*,
    -- normalized: share of this identifier's effort going to this observer
    b.n_ids::FLOAT / it.identifier_total_ids       AS identifier_effort_share,
    -- log weight dampens high-frequency edges (prolific pairs)
    LN(1 + b.n_ids)                                AS log_weight
FROM base b
JOIN identifier_totals it USING (identifier_id);


CREATE OR REPLACE TABLE graph.user_nodes AS
SELECT user_id, 'observer'   AS role FROM staged.observations
UNION
SELECT user_id, 'identifier' AS role FROM staged.identifications WHERE own_observation IS FALSE
-- users appear in both roles naturally; handle with aggregation if needed
