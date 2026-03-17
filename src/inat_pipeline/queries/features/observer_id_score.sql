
CREATE OR REPLACE TABLE features.obs_id_score AS

SELECT

    o.id AS observation_id,
    o.user_id,
    COUNT(DISTINCT i.identification_id)         AS prior_ids_given_by_observer,
    COUNT(DISTINCT i.observation_id)            AS prior_obs_they_identified,
    COUNT(DISTINCT i.observer_id)              AS prior_unique_observers_helped,
    COUNT(DISTINCT i.taxon_id)                  AS prior_taxa_identified,
    MAX(identifier_rg_rate_at_t)                AS  prior_rg_rate_of_ids_given


FROM staged.observations o
JOIN features.identifiers i
    ON i.observation_id = o.id
GROUP BY o.id, o.user_id
