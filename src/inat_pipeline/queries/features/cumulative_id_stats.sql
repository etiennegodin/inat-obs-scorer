CREATE OR REPLACE TABLE features.cumulative_id_stats AS
SELECT
    obs.id AS observation_id,
    obs.user_id,
    t.cumulative_ids_received                 AS prior_ids_received,
    t.cumulative_distinct_identifiers         AS prior_identifier_diversity,
    t.cumulative_ids_supporting               AS prior_ids_supporting,
    t.cumulative_ids_improving                AS prior_ids_improving,
    t.cumulative_ids_leading                  AS prior_ids_leading,
    t.cumulative_ids_maverick                 AS prior_ids_maverick,
    t.cumulative_ids_vision                   AS prior_ids_vision

FROM staged.observations obs
ASOF JOIN graph.observer_timeline t
    ON  t.observer_id  = obs.user_id
    AND t.created_at  <= obs.created_at     -- get the snapshot just before submission

;
