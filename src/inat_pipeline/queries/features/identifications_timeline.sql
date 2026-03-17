CREATE OR REPLACE TABLE graph.observer_id_events AS

WITH raw_events AS (
    SELECT
        o.user_id        AS observer_id,
        i.user_id        AS identifier_id,
        i.created_at,
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
    observer_id,
    identifier_id,
    created_at,
    1                                                     AS n_ids,
    CASE WHEN pair_occurrence_rank = 1 THEN 1 ELSE 0 END AS is_new_identifier
    -- is_new_identifier = 1 only on the FIRST time this person ever IDed for this observer
    -- this lets you approximate a running DISTINCT count with SUM()
FROM raw_events;

CREATE OR REPLACE TABLE graph.observer_timeline AS
SELECT
    observer_id,
    identifier_id,
    created_at,
    SUM(n_ids) OVER (
        PARTITION BY observer_id
        ORDER BY created_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_ids_received,

    SUM(is_new_identifier) OVER (
        PARTITION BY observer_id
        ORDER BY created_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_distinct_identifiers,

    SUM(n_ids) OVER (
        PARTITION BY observer_id, identifier_id
        ORDER BY created_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_pair_weight

FROM graph.observer_id_events
ORDER BY observer_id, created_at ASC;


CREATE INDEX IF NOT EXISTS timeline_index ON graph.observer_timeline (observer_id, created_at);
