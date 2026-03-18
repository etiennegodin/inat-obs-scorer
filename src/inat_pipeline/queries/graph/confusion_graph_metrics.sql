CREATE OR REPLACE TABLE graph.confusion_2_hop_size AS

SELECT focal.taxon_id,
    COUNT(DISTINCT edge.taxon_id || '-' || edge.similar_taxon_id) AS neighbor_cross_edges
FROM GRAPH_TABLE(confusion_graph
    MATCH (focal:taxon)-[:similar_to]->(a:taxon)-[:similar_to]->(b:taxon)
    WHERE (focal)-[:similar_to]->(b)  -- b is also a direct neighbor of focal
    COLUMNS (focal.taxon_id, a.taxon_id, b.taxon_id)
)
GROUP BY focal.taxon_id;
