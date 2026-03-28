CREATE OR REPLACE TABLE staged.histogram_scraped AS

SELECT
    raw_id AS taxon_id,
    (raw_json -> 'week_of_year')::MAP (INT, INT) AS week_map_observed
FROM raw.obs_histogram_na_observed;
