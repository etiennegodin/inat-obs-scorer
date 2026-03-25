CREATE OR REPLACE TABLE features.label AS

SELECT
    * EXCLUDE (is_rg),
    is_rg AS label

FROM features.model_population
JOIN research_grade_windowed(to_days(:label_window_days))
