-- features/identifications_at_window.sql


CREATE OR REPLACE TABLE features.identifications_at_window AS
WITH params AS (

    SELECT

    to_days(:score_window_days) AS score_window_days,

),

window_ids AS (

    SELECT
        i.observation_id,
        i.user_id,
        i.created_at,
        i.category,

        -- Sequence within window, oldest ID first — used to isolate first-ID signal
        ROW_NUMBER() OVER (
            PARTITION BY i.observation_id
            ORDER BY i.created_at
        ) AS id_seq

    FROM staged.identifications i
    CROSS JOIN params p
    JOIN staged.observations     o ON i.observation_id = o.id

    WHERE i."current" IS TRUE
      AND i.own_observation IS FALSE                              -- external IDs only
      AND i.created_at <= o.created_at + p.score_window_days

),

first_id AS (

    -- First external ID within the score window
    SELECT
        observation_id,
        created_at  AS first_id_at,
        category    AS first_id_category
    FROM window_ids
    WHERE id_seq = 1

)

SELECT
    o.id        AS observation_id,
    -- ── First-ID signals ─────────────────────────────────────────────────────
    f.first_id_at IS NOT NULL                               AS has_any_id,

FROM staged.observations o
LEFT JOIN first_id      f  ON f.observation_id = o.id
;
