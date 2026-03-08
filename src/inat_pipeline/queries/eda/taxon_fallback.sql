
CREATE SCHEMA IF NOT EXISTS eda;

CREATE OR REPLACE TABLE eda.obs_per_taxon AS
WITH agg AS(
    SELECT o.taxon_id,

    COUNT(*) as obs_per_taxon


    FROM staged.observations o
    GROUP BY o.taxon_id
    ORDER BY obs_per_taxon ASC
)

SELECT *,

CASE WHEN obs_per_taxon < 30 THEN TRUE ELSE FALSE END AS fallback
FROM agg 
