SET search_path = 'staged,main';

-- Single hop for clustering
CREATE OR REPLACE TABLE graph.single_hop AS
--SELECT taxon_id, COUNT(DISTINCT(n_taxon_id)) AS hop_neighborhood_size
FROM GRAPH_TABLE(confusion_graph
    MATCH (a:taxon)-[s:similar_to]->(b:taxon)
    COLUMNS (a.taxon_id, b.taxon_id as n_taxon_id)
);

-- Double hop neighborhood stats
CREATE OR REPLACE TABLE graph.double_hop_stats AS
SELECT focal_taxon_id as taxon_id,
COUNT(DISTINCT(n_taxon_id)) AS double_hop_nbrhd_size,
COUNT(DISTINCT(n_genus_id)) AS double_hop_nbrhd_genus,
COUNT(DISTINCT(n_family_id)) AS double_hop_nbrhd_family,

COUNT(*) FILTER (WHERE focal_genus_id !=  n_genus_id) AS genus_crossover_count,
COUNT(*) FILTER (WHERE focal_family_id !=  n_family_id) AS family_crossover_count,

FROM GRAPH_TABLE(confusion_graph
    MATCH (a:taxon)-[s1:similar_to]->(b:taxon)-[s2:similar_to]->(c:taxon)
    COLUMNS (a.taxon_id as focal_taxon_id,
        a.genus_id as focal_genus_id,
        a.family_id as focal_family_id,
        c.taxon_id AS n_taxon_id,
        c.genus_id AS n_genus_id,
        c.family_id AS n_family_id)
) GROUP BY focal_taxon_id
;

-- Clustering coefficient

CREATE OR REPLACE TABLE graph.clustering_coefficient AS

WITH neighbor_pairs AS (
    -- All pairs (b, c) where both are neighbors of focal a
    SELECT h1.taxon_id AS focal, h1.n_taxon_id AS b, h2.n_taxon_id AS c
    FROM graph.single_hop h1
    JOIN graph.single_hop h2 ON h2.taxon_id = h1.taxon_id
    WHERE h1.n_taxon_id != h2.n_taxon_id
),

connected_pairs AS (
    -- Which of those pairs have an edge between them?
    SELECT p.focal, COUNT(*) AS connected
    FROM neighbor_pairs p
    JOIN graph.single_hop e ON e.taxon_id = p.b AND e.n_taxon_id = p.c
    GROUP BY p.focal
),
total_pairs AS (
    SELECT focal, COUNT(*) AS total
    FROM neighbor_pairs
    GROUP BY focal
)

SELECT
    t.focal AS taxon_id,
    ROUND(COALESCE(c.connected, 0)::FLOAT / NULLIF(t.total, 0), 4) AS clustering_coefficient
FROM total_pairs t
LEFT JOIN connected_pairs c ON c.focal = t.focal
