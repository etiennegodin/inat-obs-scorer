CREATE OR REPLACE TABLE tests.reasearch_grade AS

SELECT 
    rg.observation_id,
    rg.is_rg,
    o.quality_grade


FROM research_grade_windowed(INTERVAL '999 years') rg
LEFT JOIN staged.observations o ON o.id = rg.observation_id;



CREATE OR REPLACE TABLE tests.reasearch_grade_cm AS

SELECT 

    COUNT(CASE WHEN o.quality_grade = 'research' AND rg.is_rg THEN 1 END) AS True_Positive,
    COUNT(CASE WHEN o.quality_grade != 'research' AND rg.is_rg THEN 1 END) AS False_Positive,
    COUNT(CASE WHEN o.quality_grade = 'research' AND NOT rg.is_rg THEN 1 END) AS False_Negative,
    COUNT(CASE WHEN o.quality_grade != 'research' AND NOT rg.is_rg THEN 1 END) AS True_Negative


FROM research_grade_windowed(INTERVAL '999 years') rg
LEFT JOIN staged.observations o ON o.id = rg.observation_id;

