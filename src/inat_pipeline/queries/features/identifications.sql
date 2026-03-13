CREATE OR REPLACE TABLE staged.ids_received_summary AS
    SELECT
        observation_id,
        COUNT(*)                                        AS total_ids,
        COUNT(*) FILTER (WHERE category = 'supporting') AS agreeing_ids,
        COUNT(*) FILTER (WHERE category = 'improving')  AS improving_ids,
        COUNT(*) FILTER (WHERE category = 'maverick')   AS maverick_ids,
        COUNT(*) FILTER (WHERE vision IS TRUE)          AS vision_ids,
        MIN(created_at)                                 AS first_id_at
    FROM staged.identifications
    WHERE own_observation IS FALSE
    GROUP BY observation_id;


CREATE OR REPLACE TABLE features.identifications AS

WITH base_obs AS(
    SELECT
        o.id                      AS observation_id,
        o.user_id,
        o.created_at,
        s.total_ids,
        s.agreeing_ids,
        s.improving_ids,
        s.maverick_ids,
        s.vision_ids,
        s.first_id_at

    FROM staged.observations o
    LEFT JOIN staged.ids_received_summary s ON s.observation_id = o.id

)

SELECT
    o.observation_id,
    o.user_id,

    -- Sum up pre-aggregated counts from all prior observations
    SUM(prev.total_ids)     AS ids_received_total,
    SUM(prev.agreeing_ids)  AS ids_received_agreeing,
    SUM(prev.improving_ids) AS ids_received_improving,
    SUM(prev.maverick_ids)  AS ids_received_maverick,
    SUM(prev.vision_ids)    AS ids_received_vision

FROM base_obs o
JOIN base_obs prev
    ON  prev.user_id    = o.user_id
    AND prev.created_at < o.created_at  -- prior observations only
GROUP BY o.observation_id, o.user_id;
