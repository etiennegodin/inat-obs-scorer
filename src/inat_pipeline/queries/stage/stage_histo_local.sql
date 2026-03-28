CREATE OR REPLACE TABLE staged.histogram_local AS

WITH created_hist AS (
    SELECT * FROM histogram_local(created_at)
),

observed_hist AS (
    -- We exclude the taxon_id here to keep the final SELECT clean
    SELECT * FROM histogram_local(observed_on)
)

SELECT
    c.taxon_id,
    c.full_year_vector AS week_map_created,
    c.total_obs AS total_obs_created,
    o.full_year_vector AS week_map_observed,
    o.total_obs AS total_obs_observed,
FROM created_hist c
JOIN observed_hist o ON c.taxon_id = o.taxon_id;
