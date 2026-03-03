-- Count of disagreeing identifications
SELECT
    observation_id,
    COUNT(*) FILTER (WHERE disagreement = TRUE) AS num_disagreements,
    COUNT(*) AS total_ids,
    (COUNT(*) FILTER (WHERE disagreement = TRUE)) * 1.0 / COUNT(*) AS disagreement_ratio
FROM identifications
GROUP BY observation_id;