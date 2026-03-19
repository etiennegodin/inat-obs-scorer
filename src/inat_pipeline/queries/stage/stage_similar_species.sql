CREATE SCHEMA IF NOT EXISTS staged;

-- 1 Unpack data
CREATE OR REPLACE TABLE staged.similar_species AS

WITH unpacked AS (
    SELECT
    taxon_id::BIGINT AS taxon_id,
    data."count" AS "count",
    UNNEST(data.taxon, RECURSIVE := true)

    FROM (
        SELECT raw_id AS taxon_id,
        from_json(
            raw_json,
            '{
                "taxon":{
                    "id":"BIGINT",
                },
                "count":"BIGINT",
            }'
            ) AS data
            FROM raw.api_similar_species
        )
)

SELECT * EXCLUDE('id'),

id AS similar_taxon_id
FROM unpacked;


-- 2 Compute taxa assymetry


CREATE OR REPLACE TABLE staged.taxa_assymetry AS
WITH child_nodes AS(
    SELECT DISTINCT similar_taxon_id AS node_id
    FROM staged.similar_species
),

parent_nodes AS(
    SELECT DISTINCT taxon_id AS node_id
    FROM staged.similar_species
),

nodes AS (
    SELECT node_id FROM child_nodes
    UNION
    SELECT node_id FROM parent_nodes
),

out_degree AS(
    SELECT
    taxon_id AS node_id,
    COUNT(similar_taxon_id) AS out_degree
    FROM staged.similar_species
    GROUP BY taxon_id
),

in_degree AS (
    SELECT
    similar_taxon_id AS node_id,
    COUNT(taxon_id) AS in_degree
    FROM staged.similar_species
    GROUP BY similar_taxon_id
)

SELECT n.node_id AS taxon_id,
o.out_degree,        -- how often a taxon is confused for something else
i.in_degree,         -- how often a taxon is the one being confused
i.in_degree - o.out_degree AS magnet_score
FROM nodes n
LEFT JOIN out_degree o ON o.node_id = n.node_id
LEFT JOIN in_degree i ON i.node_id = n.node_id;

-- 3 Compute taxa distance

CREATE OR REPLACE TABLE staged.taxa_distance AS

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
