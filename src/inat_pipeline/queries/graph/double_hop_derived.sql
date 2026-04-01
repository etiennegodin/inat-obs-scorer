-- Step 3: exclusive 2-hop via hash anti-join on two small materialized tables
CREATE OR REPLACE TABLE graph.exclusive_2hop AS
SELECT
    d.taxon_id,
    COUNT(DISTINCT d.similar_taxon_id) AS exclusive_2hop_size
FROM graph.double_hop_pairs d
LEFT JOIN graph.single_hop s
    ON  s.taxon_id      = d.taxon_id
    AND s.similar_taxon_id = d.similar_taxon_id
WHERE s.similar_taxon_id IS NULL   -- not reachable at hop 1
GROUP BY d.taxon_id;


CREATE OR REPLACE TABLE graph.double_hop_derived AS

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
LEFT JOIN graph.exclusive_2hop e   USING (taxon_id)
