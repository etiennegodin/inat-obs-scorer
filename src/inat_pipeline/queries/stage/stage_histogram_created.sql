ALTER TABLE staged.histogram_scraped ADD COLUMN IF NOT EXISTS week_map_created MAP (INT, INT);


UPDATE staged.histogram_scraped

SET week_map_created = (s.raw_json -> 'week_of_year')::MAP (INT, INT)
FROM raw.obs_histogram_na_created AS s
WHERE staged.histogram_scraped.taxon_id = s.raw_id;
