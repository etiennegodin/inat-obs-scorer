SET search_path = 'staged,main';


-- Step 1: materialize 2-hop pairs as a table first (don't compute exclusive inline)
CREATE OR REPLACE TABLE graph.double_hop_pairs AS
SELECT DISTINCT focal_taxon_id AS taxon_id, similar_taxon_id
FROM GRAPH_TABLE(confusion_graph
    MATCH (a:taxon)-[s1:similar_to]->(b:taxon)-[s2:similar_to]->(c:taxon)
    COLUMNS (a.taxon_id AS focal_taxon_id, c.taxon_id AS similar_taxon_id)
)
WHERE focal_taxon_id != similar_taxon_id;  -- exclude self-loops early


-- Double hop neighborhood stats
CREATE OR REPLACE TABLE graph.double_hop AS
SELECT focal_taxon_id as taxon_id,
COUNT(DISTINCT(similar_taxon_id)) AS double_hop_nbrhd_size,
COUNT(DISTINCT(n_genus_id)) AS double_hop_nbrhd_genus_count,
COUNT(DISTINCT(n_family_id)) AS double_hop_nbrhd_family_count,

COUNT(*) FILTER (WHERE focal_genus_id !=  n_genus_id) AS genus_crossover_count,
COUNT(*) FILTER (WHERE focal_family_id !=  n_family_id) AS family_crossover_count,

ROUND(
    COUNT(*) FILTER (WHERE focal_genus_id != n_genus_id)::FLOAT
    / NULLIF(COUNT(DISTINCT similar_taxon_id), 0), 4
) AS double_hop_genus_crossover_rate,

MAX(CASE
    WHEN focal_genus_id  = n_genus_id  THEN 1
    WHEN focal_family_id = n_family_id THEN 2
    WHEN focal_order_id  = n_order_id  THEN 3
    ELSE 4
END) AS double_hop_max_boundary_crossed

FROM GRAPH_TABLE(confusion_graph
    MATCH (a:taxon)-[s1:similar_to]->(b:taxon)-[s2:similar_to]->(c:taxon)
    COLUMNS (        a.taxon_id  AS focal_taxon_id,
        a.genus_id  AS focal_genus_id,
        a.family_id AS focal_family_id,
        a.order_id  AS focal_order_id,
        c.taxon_id  AS similar_taxon_id,
        c.genus_id  AS n_genus_id,
        c.family_id AS n_family_id,
        c.order_id  AS n_order_id)
) GROUP BY focal_taxon_id;
