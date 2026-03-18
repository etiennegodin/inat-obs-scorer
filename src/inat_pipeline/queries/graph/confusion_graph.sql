CREATE OR REPLACE PROPERTY GRAPH confusion_graph
VERTEX TABLES (staged.taxa LABEL taxon)
EDGE TABLES (
    staged.similar_species
        SOURCE KEY (taxon_id)      REFERENCES staged.taxa (taxon_id)
        DESTINATION KEY (similar_taxon_id) REFERENCES staged.taxa (taxon_id)
    LABEL similar_to
);
