-- created at week local histo
CREATE OR REPLACE TABLE staged.histogram_created_local AS

WITH base AS (

    SELECT
        id AS observation_id,
        taxon_id,
        WEEK(created_at) - 1 AS week_id,
    FROM staged.observations
),

SELECT
    taxon_id,
    HISTOGRAM(week_id) OVER taxon_history AS hist,

    COUNT(*) OVER taxon_history
        AS total_obs

FROM base
WINDOW taxon_history AS (
    PARTITION BY taxon_id
)
