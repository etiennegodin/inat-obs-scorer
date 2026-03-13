CREATE OR REPLACE TABLE features.identifications AS

WITH base_obs AS(
    SELECT
        o.id                      AS observation_id,
        o.created_at              AS o_created_at,
        i.id                      AS identification_id,
        i.created_at              AS i_created_at,
        i.*EXCLUDE(id, created_at),
        -- Honest RG label from macro (no leakage)
        COALESCE(rg.is_rg, FALSE)           AS is_rg

    FROM staged.observations o
    LEFT JOIN staged.identifications i ON o.id = i.observation_id
    -- Unbounded window = all identifications ever, for current-state scoring
    LEFT JOIN research_grade_windowed(INTERVAL '999 years') rg
        ON rg.observation_id = o.id
)

SELECT observation_id,

COUNT(DISTINCT identification_id) OVER AS ids_history id_count_total,
COUNT(DISTINCT identification_id) FILTER (WHERE own_observation IS FALSE) AS id_count_community,
COUNT(DISTINCT identification_id) FILTER (WHERE category = 'supporting') AS id_count_agreeing,
COUNT(DISTINCT identification_id) FILTER (WHERE category = 'improving') AS id_count_improving,
COUNT(DISTINCT identification_id) FILTER (WHERE category = 'maverick') AS id_count_maverick,
COUNT(DISTINCT identification_id) FILTER (WHERE vision IS TRUE) AS id_count_vision_used,
MIN(i_created_at) FILTER (WHERE own_observation IS FALSE) - o_created_at AS time_to_first_community_id_hours

FROM base_obs

WINDOW
    ids_history AS (
        PARTITION BY observation_id
        ORDER BY i_created_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )
