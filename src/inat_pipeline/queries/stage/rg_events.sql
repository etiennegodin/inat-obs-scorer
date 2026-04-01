CREATE OR REPLACE TABLE staged.rg_events AS
SELECT * FROM research_grade_windowed(INTERVAL '999 years');
