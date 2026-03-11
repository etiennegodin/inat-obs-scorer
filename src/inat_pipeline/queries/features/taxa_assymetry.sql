CREATE OR REPLACE TABLE features.taxa_assymetry AS
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
LEFT JOIN in_degree i ON i.node_id = n.node_id
