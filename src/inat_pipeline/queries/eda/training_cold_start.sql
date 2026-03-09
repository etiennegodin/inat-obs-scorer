
-- Check 3: Cold start prevalence
SELECT
    split,
    ROUND(AVG(taxon_cold_start::FLOAT), 3)    AS pct_taxon_cold,
    --ROUND(AVG(observer_cold_start::FLOAT), 3) AS pct_observer_cold
FROM   features.training
GROUP BY split;
-- High cold start % in test vs train signals a new-taxon / new-observer
-- distribution shift worth flagging before deployment
```