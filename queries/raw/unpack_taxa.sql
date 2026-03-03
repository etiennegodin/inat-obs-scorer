CREATE OR REPLACE TABLE staged.taxa AS

SELECT id,
phylum,
class,
'order',
family,
genus,
specificEpithet as species,
taxonRank

FROM raw.taxa
WHERE kingdom = 'Plantae'
