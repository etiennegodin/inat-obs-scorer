FORCE INSTALL duckpgq FROM community;
LOAD duckpgq;

SET search_path = 'staged,main';

DESCRIBE PROPERTY GRAPH confusion_graph;

FROM GRAPH_TABLE(confusion_graph
    MATCH (a:taxon WHERE a.taxon_id = 564969)-[k:similar_to]->(b:taxon)
    COLUMNS (b.taxon_id)
);
