CREATE OR REPLACE TABLE staged.identifiers_summary AS
    SELECT
        observation_id,
        COUNT(DISTINCT(user_id))                        AS total_identifiers,
        COUNT(DISTINCT(user_id)) FILTER (WHERE category = 'supporting') AS agreeing_identifiers,
        COUNT(DISTINCT(user_id)) FILTER (WHERE category = 'improving')  AS improving_identifiers,
        COUNT(DISTINCT(user_id)) FILTER (WHERE category = 'maverick')   AS maverick_identifiers,
        COUNT(DISTINCT(user_id)) FILTER (WHERE vision IS TRUE)          AS vision_identifiers,
    FROM staged.identifications
    WHERE own_observation IS FALSE
    GROUP BY observation_id;


CREATE OR REPLACE TABLE features.identifiers AS

WITH base_obs AS(
    SELECT
        o.id                      AS observation_id,
        o.user_id,
        o.created_at,
        s.total_identifiers,
        s.agreeing_identifiers,
        s.improving_identifiers,
        s.maverick_identifiers,
        s.vision_identifiers,

    FROM staged.observations o
    LEFT JOIN staged.identifiers_summary s ON s.observation_id = o.id

)

SELECT
    o.observation_id,
    o.user_id,

    -- Sum up pre-aggregated counts from all prior observations
    SUM(prev.total_identifiers)     AS identifiers_total,
    SUM(prev.agreeing_identifiers)  AS identifiers_agreeing,
    SUM(prev.improving_identifiers) AS identifiers_improving,
    SUM(prev.maverick_identifiers)  AS identifiers_maverick,
    SUM(prev.vision_identifiers)    AS identifiers_vision

FROM base_obs o
JOIN base_obs prev
    ON  prev.user_id    = o.user_id
    AND prev.created_at < o.created_at  -- prior observations only
GROUP BY o.observation_id, o.user_id;
