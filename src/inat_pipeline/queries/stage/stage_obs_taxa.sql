
-- Unpacking raw data
CREATE OR REPLACE TABLE staged.taxa_raw AS

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

-- Creating enriched version with taxon_id instead of names per taxonomic level
CREATE OR REPLACE TABLE staged.taxa AS

WITH rank_ids AS (
    SELECT
        t.id            AS taxon_id,
        t."taxonRank"   AS rank,
        t.phylum,
        t.class,
        t."order",
        t.family,
        t.genus,
        t.species,

        -- Resolve each ancestor name to its canonical taxon_id
        -- by joining back to taxa at each rank level
        phylum_t.id     AS phylum_id,
        class_t.id      AS class_id,
        order_t.id      AS order_id,
        family_t.id     AS family_id,
        genus_t.id      AS genus_id,
        species_t.id    AS species_id,

        -- Numeric rank level for ordering (lower = more specific)
        CASE t."taxonRank"
            -- Sub-species level (more specific than species, still RG-eligible)
            WHEN 'subspecies'   THEN 5
            WHEN 'variety'      THEN 5
            WHEN 'form'         THEN 5
            WHEN 'infrahybrid'  THEN 5

            WHEN 'species'      THEN 10
            WHEN 'hybrid'       THEN 10  -- treat AS species-equivalent
            WHEN 'genushybrid'  THEN 10  -- debatable, see note below

            WHEN 'complex'      THEN 10  -- species complex, sits just above species

            -- Genus group
            WHEN 'genus'        THEN 20
            WHEN 'subgenus'     THEN 20
            WHEN 'section'      THEN 20
            WHEN 'subsection'   THEN 20

            -- Family group
            WHEN 'family'       THEN 30
            WHEN 'subfamily'    THEN 30
            WHEN 'tribe'        THEN 30
            WHEN 'supertribe'   THEN 30
            WHEN 'subtribe'     THEN 30

            -- Order group
            WHEN 'order'        THEN 40
            WHEN 'suborder'     THEN 40

            -- Class group
            WHEN 'class'        THEN 50
            WHEN 'subclass'     THEN 50

            WHEN 'phylum'       THEN 60
            WHEN 'subphylum'    THEN 60

            WHEN 'kingdom'      THEN 70

            ELSE 99  -- unknown, treat AS coarsest
        END AS rank_level

    FROM staged.taxa_raw t

    -- Each join resolves name → id at that rank
    LEFT JOIN staged.taxa_raw phylum_t
        ON phylum_t.species IS NULL
        AND phylum_t.genus   IS NULL
        AND phylum_t.family  IS NULL
        AND phylum_t."order" IS NULL
        AND phylum_t.class   IS NULL
        AND phylum_t.phylum  = t.phylum
        AND phylum_t."taxonRank" = 'phylum'

    LEFT JOIN staged.taxa_raw class_t
            ON class_t.species IS NULL
            AND class_t.genus   IS NULL
            AND class_t.family  IS NULL
            AND class_t."order" IS NULL
            AND class_t.class   = t.class
            AND class_t."taxonRank" = 'class'

    LEFT JOIN staged.taxa_raw order_t
        ON order_t.species IS NULL
        AND order_t.genus   IS NULL
        AND order_t.family  IS NULL
        AND order_t."order" = t."order"
        AND order_t."taxonRank" = 'order'

    LEFT JOIN staged.taxa_raw family_t
        ON family_t.species IS NULL
        AND family_t.genus   IS NULL
        AND family_t.family  = t.family
        AND family_t."taxonRank" = 'family'

    LEFT JOIN staged.taxa_raw genus_t
        ON genus_t.species IS NULL
        AND genus_t.genus   = t.genus
        AND genus_t."taxonRank" = 'genus'

    LEFT JOIN staged.taxa_raw species_t
        ON species_t.species = t.species
        AND species_t.genus  = t.genus      -- genus scopes species to avoid homonyms
        AND species_t."taxonRank" = 'species'

)
SELECT * FROM rank_ids;

# Indexing
CREATE INDEX IF NOT EXISTS idx_taxo_hierarchy ON staged.taxa (phylum_id, class_id, order_id, family_id, genus_id);
CREATE INDEX IF NOT EXISTS idx_genus ON staged.taxa (genus_id);
CREATE INDEX IF NOT EXISTS idx_family ON staged.taxa (family_id);
CREATE INDEX IF NOT EXISTS idx_order ON staged.taxa (order_id);

CREATE INDEX IF NOT EXISTS idx_taxa_lookup
ON staged.taxa (species_id, phylum_id, class_id, order_id, family_id, genus_id);
