-- Check 2: Split size and label rate per split

SELECT
    split,
    COUNT(*)                          AS n_obs,    
    ROUND(AVG(label::FLOAT), 3)      AS rg_rate,
    MIN(created_at)                  AS earliest,
    MAX(created_at)                  AS latest
FROM   features.training,
GROUP BY split
ORDER BY split;
-- Train should span oldest dates, test the most recent
-- RG rates should be roughly similar across splits (not drifting wildly)

