-- Average experience of identifiers
SELECT
    observation_id,
    AVG(observations_count) AS avg_user_obs_count,
    AVG(identifications_count) AS avg_user_id_count,
    AVG(species_count) AS avg_user_species_count
FROM identifications
GROUP BY observation_id;