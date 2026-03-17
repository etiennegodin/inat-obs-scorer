CREATE OR REPLACE TABLE graph.observer_id_events AS

WITH raw_events AS (
    SELECT
        i.id AS identification_id,
        i.observation_id,
        o.user_id        AS observer_id,
        i.user_id        AS identifier_id,
        i.created_at,
        i.category,
        i.vision,
        -- rank each (observer, identifier) pair occurrence chronologically
        ROW_NUMBER() OVER (
            PARTITION BY o.user_id, i.user_id
            ORDER BY i.created_at
        ) AS pair_occurrence_rank
    FROM staged.identifications i
    JOIN staged.observations o
        ON o.id = i.observation_id
    WHERE i.user_id != o.user_id
)

SELECT
    observation_id,
    id,
    observer_id,
    identifier_id,
    created_at,
    vision,
    category,
    1                                                     AS n_ids,
    CASE WHEN pair_occurrence_rank = 1 THEN 1 ELSE 0 END AS is_new_identifier
    -- is_new_identifier = 1 only on the FIRST time this person ever IDed for this observer
    -- this lets you approximate a running DISTINCT count with SUM()
FROM raw_events;

CREATE OR REPLACE TABLE graph.observer_timeline AS
SELECT
    observation_id,
    id,
    observer_id,
    identifier_id,
    created_at,
    SUM(n_ids) OVER observer_history AS cumulative_ids_received,
    SUM(is_new_identifier) OVER observer_history AS cumulative_distinct_identifiers,

    SUM(n_ids) FILTER (WHERE category = 'supporting') OVER observer_history AS cumulative_ids_supporting,
    SUM(n_ids) FILTER (WHERE category = 'improving') OVER observer_history AS cumulative_ids_improving,
    SUM(n_ids) FILTER (WHERE category = 'leading') OVER observer_history AS cumulative_ids_leading,
    SUM(n_ids) FILTER (WHERE category = 'maverick') OVER observer_history AS cumulative_ids_maverick,
    SUM(n_ids) FILTER (WHERE vision IS TRUE) OVER observer_history AS cumulative_ids_vision,





    SUM(n_ids) OVER (
        PARTITION BY observer_id, identifier_id
        ORDER BY created_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_pair_weight

FROM graph.observer_id_events

    WINDOW observer_history AS (
            PARTITION BY observer_id
            ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
ORDER BY observer_id, created_at ASC;


CREATE INDEX IF NOT EXISTS timeline_index ON graph.observer_timeline (observer_id, created_at);
