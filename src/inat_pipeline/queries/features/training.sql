CREATE OR REPLACE TABLE features.training AS
SELECT
    --Identity
    s.split,

    --Label
    l.label,

     --Documentation features (submission-time safe)
    m.photo_count,
    m.has_description,
    m.has_tags,
    m.tag_count,
    --m.has_license,
    m.positional_accuracy_m,
    m.geoprivacy IS NOT NULL                AS geoprivacy_set,
    COALESCE(m.oauth_application_id,0)      AS oauth_application_id,

     --Temporal features
    m.created_at,
    date_part('day',m.obs_to_submit_lag_days) AS obs_to_submit_lag_days,
    m.observed_week,
    m.observed_day,
    m.observed_year,

    m.submitted_hour,
    m.submitted_day,
    m.submitted_week,
    m.submitted_year,

     --Observer features (from observer_features, computed at observation time)
         --Temporal
        date_part('day',ob.observer_tenure) AS obv_tenure_days,
        ob.is_veteran AS obv_is_veteran,
        date_part('day',ob.lag_since_last_obs) AS obv_lag_days_since_last_post,

        -- Observations
        LOG(ob.observer_obs_count_at_t + 1) AS obv_obs_count_log,
        ob.observer_rg_rate_at_t AS obv_rg_rate_lifetime,
        ob.observer_reputation_raw AS obv_reputation_score,
        ob.observer_reputation_rank AS obv_reputation_rank,
        ob.rg_rate_is_reliable AS obv_rg_rate_is_reliable,

         --Identifiers interactions
        ob.n_identifiers_mean_rank AS obv_n_identifiers_mean_rank,
        ob.n_identifiers_agree_mean_rank AS obv_n_identifiers_agree_mean_rank,

         --Taxonomic
        ob.taxon_diversity_species AS obv_taxon_diversity_species,
        oe.observer_species_entropy_norm AS obv_taxon_entropy,
        ob.observer_taxon_rg_rate_at_t AS obv_taxon_rg_rate_at_t,
        ob.observer_taxon_focus_rate AS obv_taxon_focus_rate,


         --Metadata
        ob.avg_photo_count AS obv_avg_photo_count,
        ob.pct_obs_with_description AS obv_pct_obs_with_description,
        ob.pct_obs_with_license AS obv_pct_obs_with_license,
        ob.pct_obs_from_mobile AS obv_pct_obs_from_mobile,
        ob.has_orcid AS obv_has_orcid,

        --Identifiers counts from obs history
        --LOG(ii.identifiers_total + 1) AS obv_n_identifier_log,
        /*
        ii.identifiers_agreeing,
        ii.identifiers_improving,
        ii.identifiers_maverick,
        ii.identifiers_vision,
        */



        -- Observer as identifiers score
        u.observer_only,

        -- Roles stats from this observers
        /*
        COALESCE(i.prior_ids_received,0),
        COALESCE(i.prior_identifier_diversity,0),
        COALESCE(i.prior_taxa_received_on,0),
        COALESCE(i.prior_ids_received_improving,0),
        COALESCE(i.prior_ids_received_leading,0),
        COALESCE(i.prior_ids_received_maverick,0),
        COALESCE(i.prior_ids_received_supporting,0),
        COALESCE(i.prior_ids_received_vision,0),
        */
        COALESCE(i.prior_ids_given,0) AS prior_ids_given,
        COALESCE(i.prior_observers_helped,0) AS prior_obseververs_helped,
        COALESCE(i.prior_taxa_identified,0) AS prior_taxa_identified,
        /*
        COALESCE(i.prior_ids_given_improving,0),
        COALESCE(i.prior_ids_given_leading,0),
        COALESCE(i.prior_ids_given_maverick,0),
        COALESCE(i.prior_ids_given_supporting,0),
        COALESCE(i.prior_ids_given_vision,0),
        */
        COALESCE(i.reciprocity_ratio, 0) AS reciprocity_ratio,
     --Taxon features (fixed lookup)
    t.taxon_rg_rate,
    t.rank_level,
    t.taxon_popularity_rank,
    t.rg_rate_source,
    t.taxon_avg_ids_to_rg,
    t.taxon_cold_start,
    t.genus_popularity_rank,
    t.family_popularity_rank,
    t.genus_rg_rate,
    t.family_rg_rate,


     --Fixed Taxon confusion stats
    IFNULL(c.has_similar_species, FALSE)    AS has_similar_species,
    COALESCE(c.neighborhood_difficulty,0)   AS neighborhood_difficulty,
    c.rg_percentile_in_neighborhood,
    c.magnet_score,
    c.rg_rate_vs_neighbors,
    c.neighbor_genus_diversity,
    c.nbor_dist_mean,
    c.nbor_rg_rate_std,


FROM features.metadata m
JOIN features.splits                     s  ON m.observation_id = s.observation_id
LEFT JOIN features.observers             ob ON m.observation_id = ob.observation_id
LEFT JOIN features.observers_entropy     oe ON m.observation_id = oe.observation_id
LEFT JOIN features.label                 l  ON m.observation_id = l.observation_id

--LEFT JOIN features.identifications       i  ON m.observation_id = i.observation_id
--LEFT JOIN features.identifiers           ir ON m.observation_id = ii.observation_id
JOIN features.identifications            i  ON m.observation_id = i.observation_id
LEFT JOIN features.taxon                 t  ON m.observation_id = t.observation_id
LEFT JOIN features.taxa_confusion        c  ON m.taxon_id = c.taxon_id
LEFT JOIN staged.users                   u  ON m.user_id = u.user_id

WHERE m.label IS NOT NULL
ORDER BY m.created_at
