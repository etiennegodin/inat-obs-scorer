CREATE OR REPLACE TABLE features.cumulative_id_stats AS
SELECT
    obs.id AS observation_id,
    obs.user_id,
    t.cumulative_ids_received                 AS prior_ids_received,
    t.cumulative_distinct_identifiers         AS prior_identifier_diversity
FROM staged.observations obs
ASOF JOIN graph.observer_timeline t
    ON  t.observer_id  = obs.user_id
    AND t.created_at  <= obs.created_at     -- get the snapshot just before submission

;
