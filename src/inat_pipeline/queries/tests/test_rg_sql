CREATE OR REPLACE TABLE tests.reasearch_grade AS

SELECT 
    rg.observation_id,
    rg.is_rg,
    o.quality_grade,
    CASE 
        WHEN rg.is_rg AND o.quality_grade = 'research' THEN 'TP'
        WHEN rg.is_rg AND o.quality_grade != 'research' THEN 'FP'
        WHEN NOT rg.is_rg AND o.quality_grade != 'research' THEN 'TN'
        WHEN NOT rg.is_rg AND o.quality_grade = 'research' THEN 'FN'

    END AS label


FROM research_grade_windowed(INTERVAL '999 years') rg
LEFT JOIN staged.observations o ON o.id = rg.observation_id;



