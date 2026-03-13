-- Check 2: Split size and label rate per split

SELECT
    s.split,
    COUNT(s.observation_id)                        AS n_obs,
    ROUND(AVG(l.label::FLOAT), 3)      AS pos_rate

FROM   tests.splits s
JOIN features.label l on s.observation_id = l.observation_id
GROUP BY split
ORDER BY n_obs DESC;
