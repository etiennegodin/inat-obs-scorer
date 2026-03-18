CREATE OR REPLACE TABLE features.identifications AS

-- Observer features: history of IDs RECEIVED by this observation's author
WITH observer_features AS(
    SELECT
        obs.observation_id,
        obs.user_id,
        t.cumulative_events                 AS prior_ids_received,
        t.cumulative_distinct_counterparts  AS prior_identifier_diversity,
        t.cumulative_distinct_taxa          AS prior_taxa_received_on,
        t.cumulative_improving AS prior_ids_received_improving,
        t.cumulative_leading AS prior_ids_received_leading,
        t.cumulative_maverick AS prior_ids_received_maverick,
        t.cumulative_supporting AS prior_ids_received_supporting,
        t.cumulative_vision AS prior_ids_received_vision,


        -- Bayesian-shrunk RG rate as observer (how often do their obs reach RG)
        (t.cumulative_settled_rg + 10 * obs.expected_rg_rate)
            / NULLIF(t.cumulative_settled_events + 10, 0)
                                            AS prior_observer_rg_rate
    FROM features.observations obs
    ASOF JOIN graph.user_role_timeline t
        ON  t.user_id     = obs.user_id
        AND t.role        = 'observer'
        AND t.created_at <= obs.created_at
),

identifier_features AS(
    SELECT
        obs.observation_id,
        obs.user_id,
        t.cumulative_events                 AS prior_ids_given,
        t.cumulative_distinct_counterparts  AS prior_observers_helped,
        t.cumulative_distinct_taxa          AS prior_taxa_identified,
        t.cumulative_improving AS prior_ids_given_improving,
        t.cumulative_leading AS prior_ids_given_leading,
        t.cumulative_maverick AS prior_ids_given_maverick,
        t.cumulative_supporting AS prior_ids_given_supporting,
        t.cumulative_vision AS prior_ids_given_vision,

        -- Bayesian-shrunk RG rate as observer (how often do their obs reach RG)
        -- Shrunk RG success rate as an identifier
        (COALESCE(t.cumulative_settled_rg, 0) + 10 * obs.expected_rg_rate)
            / NULLIF(COALESCE(t.cumulative_settled_events, 0) + 10, 0)
                                                                    AS prior_identifier_rg_rate,
    FROM features.observations obs
    ASOF JOIN graph.user_role_timeline t
        ON  t.user_id     = obs.user_id
        AND t.role        = 'identifier'
        AND t.created_at <= obs.created_at
)



/*
## What the Two RG Rates Mean


| Feature | Interpretation |
|---|---|
| `prior_observer_rg_rate` | How reliably does this person's observations close? Quality / completeness signal |
| `prior_identifier_rg_rate` | How often do this person's IDs push observations to RG? Expertise / correctness signal |
| `prior_observer_rg_rate` (via identifier role join) | When identifying for others, what's the RG rate of obs they chose to engage with? Selectivity signal |

That last one is s

*/

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
