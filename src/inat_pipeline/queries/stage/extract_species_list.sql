CREATE OR REPLACE TABLE staged.species_list AS
SELECT
    --Keys
    DISTINCT(taxon_id),
    -- Human readable info
    genus,species,
    rank
FROM staged.observations
WHERE rank_level <= 20 -- genus level and lower
