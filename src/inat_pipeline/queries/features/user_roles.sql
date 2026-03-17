CREATE OR REPLACE TABLE features.user_roles AS

-- Observer features: history of IDs RECEIVED by this observation's author
WITH observer_features AS(
    SELECT
        obs.id AS observation_id,
        obs.user_id,
        t.cumulative_events                 AS prior_ids_received,
        t.cumulative_distinct_counterparts  AS prior_identifier_diversity,
        t.cumulative_distinct_taxa          AS prior_taxa_received_on,
        t.cumulative_improving AS prior_ids_received_improving,
        t.cumulative_leading AS prior_ids_received_leading,
        t.cumulative_maverick AS prior_ids_received_maverick,
        t.cumulative_supporting AS prior_ids_received_supporting,
        t.cumulative_vision AS prior_ids_received_vision,
    FROM staged.observations obs
    ASOF JOIN graph.user_role_timeline t
        ON  t.user_id     = obs.user_id
        AND t.role        = 'observer'
        AND t.created_at <= obs.created_at
),

identifier_features AS(
    SELECT
        obs.id AS observation_id,
        obs.user_id,
        t.cumulative_events                 AS prior_ids_given,
        t.cumulative_distinct_counterparts  AS prior_observers_helped,
        t.cumulative_distinct_taxa          AS prior_taxa_identified,
        t.cumulative_improving AS prior_ids_given_improving,
        t.cumulative_leading AS prior_ids_given_leading,
        t.cumulative_maverick AS prior_ids_given_maverick,
        t.cumulative_supporting AS prior_ids_given_supporting,
        t.cumulative_vision AS prior_ids_given_vision,
    FROM staged.observations obs
    ASOF JOIN graph.user_role_timeline t
        ON  t.user_id     = obs.user_id
        AND t.role        = 'identifier'
        AND t.created_at <= obs.created_at
)


SELECT
    obs.id AS observation_id,
    -- observer-role features
    obs_role.*EXCLUDE(observation_id, user_id),
    -- identifier-role features
    id_role.*EXCLUDE(observation_id, user_id),

    --| `prior_rg_rate_of_ids_given` | How often do their IDs help close observations |


    -- derived: reciprocity ratio
    COALESCE(id_role.prior_ids_given, 0)::FLOAT
        / NULLIF(obs_role.prior_ids_received, 0)  AS reciprocity_ratio

FROM staged.observations obs
LEFT JOIN observer_features obs_role ON obs.id = obs_role.observation_id
LEFT JOIN identifier_features id_role ON obs.id = id_role.observation_id
