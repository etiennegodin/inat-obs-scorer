CREATE OR REPLACE TABLE staged.taxa AS

SELECT id,
phylum,
class,
"order",
family,
genus,
specificEpithet AS species,
taxonRank

FROM raw.taxa
WHERE kingdom = 'Plantae';


-- 1. Create the lookup table
CREATE OR REPLACE TABLE staged.taxonomy_encoded AS
SELECT 
    id AS taxon_id,
    "taxonRank",
    printf('%02d-%03d-%04d-%04d-%06d-%06d', 
        DENSE_RANK() OVER (ORDER BY phylum),
        DENSE_RANK() OVER (ORDER BY class),
        DENSE_RANK() OVER (ORDER BY "order"),
        DENSE_RANK() OVER (ORDER BY family),
        DENSE_RANK() OVER (ORDER BY genus),
        DENSE_RANK() OVER (ORDER BY species)

    ) AS taxonomy_path_code,
    DENSE_RANK() OVER (ORDER BY phylum)  AS phylum_id,
    DENSE_RANK() OVER (ORDER BY class)   AS class_id,
    DENSE_RANK() OVER (ORDER BY "order") AS order_id,
    DENSE_RANK() OVER (ORDER BY family)  AS family_id,
    DENSE_RANK() OVER (ORDER BY genus) AS genus_id,
    DENSE_RANK() OVER (ORDER BY species) AS species_id
FROM staged.taxa;
;
