-- Check 2: Split size and label rate per split

SELECT
    split,
    COUNT(*)                        AS n_obs,
    --ROUND(AVG(label::FLOAT), 3)      AS rg_rate,
    --MIN(created_at)                  AS earliest,
    --MAX(created_at)                  AS latest
FROM   tests.splits,
GROUP BY split
ORDER BY split;
