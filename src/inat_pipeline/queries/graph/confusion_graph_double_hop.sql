SET search_path = 'staged,main';

-- Double hop neighborhood stats
CREATE OR REPLACE TABLE graph.double_hop AS
SELECT focal_taxon_id as taxon_id,
COUNT(DISTINCT(similar_taxon_id)) AS double_hop_nbrhd_size,
COUNT(DISTINCT(n_genus_id)) AS double_hop_nbrhd_genus,
COUNT(DISTINCT(n_family_id)) AS double_hop_nbrhd_family,

COUNT(*) FILTER (WHERE focal_genus_id !=  n_genus_id) AS genus_crossover_count,
COUNT(*) FILTER (WHERE focal_family_id !=  n_family_id) AS family_crossover_count,

FROM GRAPH_TABLE(confusion_graph
    MATCH (a:taxon)-[s1:similar_to]->(b:taxon)-[s2:similar_to]->(c:taxon)
    COLUMNS (a.taxon_id as focal_taxon_id,
        a.genus_id as focal_genus_id,
        a.family_id as focal_family_id,
        c.taxon_id AS similar_taxon_id,
        c.genus_id AS n_genus_id,
        c.family_id AS n_family_id)
) GROUP BY focal_taxon_id;
