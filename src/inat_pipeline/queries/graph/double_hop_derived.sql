CREATE OR REPLACE TABLE graph.double_hop_derived AS
WITH exclusive_2hop AS (
    SELECT d.taxon_id, COUNT(DISTINCT d.similar_taxon_id) AS exclusive_2hop_size
        FROM (
            -- All 2-hop members (raw, before grouping)
            SELECT focal_taxon_id AS taxon_id, similar_taxon_id
            FROM GRAPH_TABLE(confusion_graph
                MATCH (a:taxon)-[s1:similar_to]->(b:taxon)-[s2:similar_to]->(c:taxon)
                COLUMNS (a.taxon_id AS focal_taxon_id, c.taxon_id AS similar_taxon_id)
            )
        ) d
        WHERE NOT EXISTS (
            SELECT 1 FROM staged.similar_species s
            WHERE s.taxon_id = d.taxon_id
            AND s.similar_taxon_id = d.similar_taxon_id
        )
        GROUP BY d.taxon_id
)

SELECT
    d.*,

    -- How much does the confusion complex expand beyond direct neighbors?
    ROUND(
        d.double_hop_nbrhd_size::FLOAT
        / NULLIF(s.similar_species_count, 0), 4
    ) AS confusion_expansion_rate,

    -- What fraction of 2-hop nodes are invisible at 1 hop?
    ROUND(
        COALESCE(e.exclusive_2hop_size, 0)::FLOAT
        / NULLIF(d.double_hop_nbrhd_size, 0), 4
    ) AS hidden_confusion_rate

FROM graph.double_hop d
JOIN graph.confusion_topology s USING (taxon_id)
LEFT JOIN exclusive_2hop e   USING (taxon_id)
