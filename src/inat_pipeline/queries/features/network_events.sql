CREATE OR REPLACE TABLE graph.network_events AS

-- Observer perspective: "I received an ID on my observation"
SELECT
    o.user_id               AS user_id,
    'observer'              AS role,
    i.user_id               AS counterpart_id,
    i.taxon_id,
    i.created_at,
    i.observation_id,
    i.vision,
    i.category,
    l.label AS is_rg,
    -- outcome is only "known" if the obs was old enough when the ID was left
    CASE WHEN i.created_at - o.created_at > INTERVAL '90 days'
         THEN 1 ELSE 0 END  AS outcome_settled

FROM staged.identifications i
JOIN staged.observations o
    ON o.id = i.observation_id
JOIN features.label l
    ON l.observation_id = i.observation_id
WHERE i.user_id != o.user_id

UNION ALL

-- Identifier perspective: "I gave an ID on someone else's observation"
SELECT
    i.user_id               AS user_id,
    'identifier'            AS role,
    o.user_id               AS counterpart_id,
    i.taxon_id,
    i.created_at,
    i.observation_id,
    i.vision,
    i.category,
    l.label AS is_rg,
    -- outcome is only "known" if the obs was old enough when the ID was left
    CASE WHEN i.created_at - o.created_at > INTERVAL '90 days'
         THEN 1 ELSE 0 END  AS outcome_settled

FROM staged.identifications i
JOIN staged.observations o
    ON o.id = i.observation_id
JOIN features.label l
    ON l.observation_id = i.observation_id
WHERE i.user_id != o.user_id;


CREATE OR REPLACE TABLE graph.network_events AS
SELECT
    *,
    -- first time this specific counterpart appeared for this user+role
    CASE WHEN ROW_NUMBER() OVER (
        PARTITION BY user_id, role, counterpart_id
        ORDER BY created_at
    ) = 1 THEN 1 ELSE 0 END  AS is_new_counterpart,

    -- first time this taxon appeared for this user+role
    CASE WHEN ROW_NUMBER() OVER (
        PARTITION BY user_id, role, taxon_id
        ORDER BY created_at
    ) = 1 THEN 1 ELSE 0 END  AS is_new_taxon

FROM graph.network_events
