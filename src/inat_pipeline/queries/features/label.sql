CREATE OR REPLACE TABLE features.label AS

SELECT *EXCLUDE(is_rg),
is_rg AS label
FROM research_grade_windowed(INTERVAL '90 days')
