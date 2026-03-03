-- Time since observation upload to first identification
SELECT
    o.id AS observation_id,
    MIN(strptime(i.created_at, '%Y-%m-%dT%H:%M:%S%z') - strptime(o.user.created_at, '%Y-%m-%dT%H:%M:%S%z')) AS time_to_first_id
FROM observations o
JOIN identifications i
ON o.id = i.observation_id
GROUP BY o.id;