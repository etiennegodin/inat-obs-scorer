CREATE OR REPLACE TABLE graph.identifiers_network AS

SELECT
    observer_id,
    COUNT(DISTINCT identifier_id)               AS identifier_diversity,
    SUM(n_ids)                                  AS total_ids_received,
    AVG(log_weight)                             AS avg_edge_strength,
    MAX(n_ids)                                  AS max_ids_from_single_identifier
FROM graph.observer_identifier_edges
GROUP BY observer_id
