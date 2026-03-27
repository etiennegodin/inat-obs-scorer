-- Single source of truth for which observations are in scope for the no-ID model

CREATE OR REPLACE TABLE features.model_population AS

WITH filtered AS (

    SELECT
        observation_id,
        is_rg

    FROM research_grade_windowed(to_days(:score_window_days))

)

SELECT b.*
FROM features.base b
JOIN filtered f
    ON b.observation_id = f.observation_id
WHERE f.is_rg = FALSE  -- no-ID population only
