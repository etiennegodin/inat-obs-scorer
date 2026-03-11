CREATE OR REPLACE TABLE features.taxa_distance AS

WITH pair_levels AS(
    SELECT
        e.taxon_id,
        e.similar_taxon_id,
        -- Get all ranks for the parent
        p.phylum_id AS p_p, p.class_id AS p_c,
        p.order_id AS p_o, p.family_id AS p_f, p.genus_id AS p_g,
        -- Get all ranks for the child
        c.phylum_id AS c_p, c.class_id AS c_c,
        c.order_id AS c_o, c.family_id AS c_f, c.genus_id AS c_g
        FROM staged.similar_species e
        JOIN staged.taxa p ON e.taxon_id = p.taxon_id
        JOIN staged.taxa c ON e.similar_taxon_id = c.taxon_id
)

-- 2. Compute distance only for these specific pairs
SELECT
    taxon_id,
    similar_taxon_id,
    CASE
        --WHEN p_k != c_k THEN 6
        WHEN p_p != c_p THEN 5
        WHEN p_c != c_c THEN 4
        WHEN p_o != c_o THEN 3
        WHEN p_f != c_f THEN 2
        WHEN p_g != c_g THEN 1
        ELSE 0
    END AS taxonomic_distance,
    CASE
        --WHEN p_k != c_k THEN 6
        WHEN p_p != c_p THEN 'phylum'
        WHEN p_c != c_c THEN 'class'
        WHEN p_o != c_o THEN 'order'
        WHEN p_f != c_f THEN 'family'
        WHEN p_g != c_g THEN 'genus'
        ELSE 'species'
    END AS taxonomic_distance_rank
FROM pair_levels
WHERE p_c = c_c; -- Optimization: Only compare within the same Phylum
