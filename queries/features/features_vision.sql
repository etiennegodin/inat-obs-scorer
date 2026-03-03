SELECT
    observation_id,
    COUNT(*) FILTER (WHERE vision = TRUE) AS num_vision_ids,
    COUNT(*) FILTER (WHERE vision = FALSE) AS num_human_ids
FROM identifications
GROUP BY observation_id;