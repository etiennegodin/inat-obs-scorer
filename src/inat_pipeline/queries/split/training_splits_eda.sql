-- Check 2: Split size and label rate per split

SELECT
    CASE
        WHEN s.split == 'train' THEN 0
        WHEN s.split == 'val' THEN 1
        WHEN s.split == 'test' THEN 2
        ELSE NULL
    END AS idx,
    s.split,

    COUNT(s.observation_id) AS n_obs,
    ROUND(AVG(l.label::FLOAT), 3) AS pos_rate

FROM features.splits s
JOIN features.label l on s.observation_id = l.observation_id
GROUP BY split
ORDER BY idx ASC;
