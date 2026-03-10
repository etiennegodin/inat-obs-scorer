CREATE OR REPLACE TABLE staged.species_list AS

SELECT DISTINCT(taxon_id),
genus,species,
rank

FROM staged.observations
WHERE rank_level <= 20
