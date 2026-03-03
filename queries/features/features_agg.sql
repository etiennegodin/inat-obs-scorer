SELECT
    id AS observation_id,
    identifications_count,
    comments_count,
    faves_count,
    array_length(reviewed_by) AS num_reviewers,
    array_length(place_ids) AS num_places
FROM observations;