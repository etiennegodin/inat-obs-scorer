CREATE OR REPLACE TABLE features.label AS

SELECT *
FROM research_grade(INTERVAL '90 days')