CREATE OR REPLACE TABLE features.label AS

WITH reasearch_grade AS (

    SELECT
        observation_id,
        is_rg AS 'label'
    FROM research_grade_windowed(to_days(:label_window_days))

)

SELECT

    mp.observation_id,
    rg.label,

FROM features.model_population mp
JOIN reasearch_grade rg ON mp.observation_id = rg.observation_id
