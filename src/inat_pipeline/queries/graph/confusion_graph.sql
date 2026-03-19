FORCE INSTALL duckpgq FROM community;
LOAD duckpgq;

DROP PROPERTY GRAPH IF EXISTS confusion_graph;

SET search_path = 'staged,main';

-- Create aliases in the default schema
CREATE OR REPLACE TABLE main.taxa_v AS SELECT * FROM staged.taxa;
CREATE OR REPLACE TABLE main.similar_v AS SELECT * FROM staged.similar_species;

-- Redefine graph using these aliases
CREATE OR REPLACE PROPERTY GRAPH confusion_graph
VERTEX TABLES (taxa_v LABEL taxon)
EDGE TABLES (
    similar_v
    SOURCE KEY (taxon_id) REFERENCES taxa_v (taxon_id)
    DESTINATION KEY (similar_taxon_id) REFERENCES taxa_v (taxon_id)
    LABEL similar_to
);

DESCRIBE PROPERTY GRAPH confusion_graph;

FROM GRAPH_TABLE(confusion_graph
    MATCH (a:taxon WHERE a.taxon = 564969)-[s:similar_to]->(b:taxon)
    COLUMNS (b.taxon_id)
);
