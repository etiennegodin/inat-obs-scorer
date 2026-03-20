CREATE OR REPLACE TABLE features.training AS
SELECT
    --Identity

    b.observation_id,
    s.split,

    --Label
    l.label,

     --Documentation features (submission-time safe)
    b.photo_count,
    b.has_description,
    b.has_tags,
    --b.tag_count,
    --b.has_license,
    b.positional_accuracy_m,
    b.geoprivacy IS NOT NULL                AS geoprivacy_set,
    COALESCE(b.oauth_application_id,0)      AS oauth_application_id,

     --Temporal features
    b.created_at,
    date_part('day',b.obs_to_submit_lag_days) AS obs_to_submit_lag_days,
    b.observed_week,
    b.observed_day,
    b.observed_year,

    b.submitted_hour,
    b.submitted_day,
    b.submitted_week,
    b.submitted_year,

     --Observer features (from observer_features, computed at observation time)
         --Temporal
        date_part('day',ob.observer_tenure) AS obv_tenure_days,
        ob.is_veteran AS obv_is_veteran,
        --date_part('day',ob.lag_since_last_obs) AS obv_lag_days_since_last_post,

        -- Observations
        LOG(ob.observer_obs_count_at_t + 1) AS obv_obs_count_log,
        ob.observer_rg_rate_at_t AS obv_rg_rate_lifetime,
        ob.observer_reputation_raw AS obv_reputation_score,
        ob.observer_reputation_rank AS obv_reputation_rank,
        ob.rg_rate_is_reliable AS obv_rg_rate_is_reliable,

         --Identifiers interactions
        ob.n_identifiers_agree_mean_rank AS obv_n_identifiers_agree_mean_rank,
        ob.n_identifiers_mean_rank AS obv_n_identifiers_mean_rank,
         --Taxonomic
        ob.taxon_diversity_species AS obv_taxon_diversity_species,
        /*
        ob.taxon_diversity_species AS obv_taxon_diversity_genus,
        ob.taxon_diversity_species AS obv_taxon_diversity_family,
        ob.taxon_diversity_species AS obv_taxon_diversity_order,
        */

        oe.observer_species_entropy_norm AS obv_taxon_entropy,
        ob.observer_taxon_rg_rate_shrunk_at_t AS obv_taxon_rg_rate_shrunk_at_t,
        ob.observer_taxon_focus_rate AS obv_taxon_focus_rate,


         --Metadata
        ob.avg_photo_count AS obv_avg_photo_count,
        ob.pct_obs_with_description AS obv_pct_obs_with_description,
        ob.pct_obs_with_license AS obv_pct_obs_with_license,
        ob.pct_obs_from_mobile AS obv_pct_obs_from_mobile,
        --ob.has_orcid AS obv_has_orcid,

        --Identifiers counts from obs history
        --LOG(ii.identifiers_total + 1) AS obv_n_identifier_log,
        /*global_rg_rate
        ii.identifiers_agreeing,
        ii.identifiers_improving,
        ii.identifiers_maverick,
        ii.identifiers_vision,
        */



        -- Observer as identifiers score
        --u.observer_only,

        -- Roles stats from this observers

        COALESCE(i.prior_ids_received,0) AS prior_ids_received,
        COALESCE(i.prior_identifier_diversity,0) AS prior_identifier_diversity,
        --COALESCE(i.prior_taxa_received_on,0) AS prior_taxa_received_on,
        COALESCE(i.prior_observer_rg_rate,0) AS prior_observer_rg_rate,
        COALESCE(i.prior_ids_received_agree_rate, 0) AS prior_ids_received_agree_rate,
        COALESCE(i.prior_ids_received_disagree_rate, 0) AS prior_ids_received_disagree_rate,

        COALESCE(i.prior_ids_given, 0) AS prior_ids_given,
        --COALESCE(i.prior_observers_helped, 0) AS prior_observers_helped,
        COALESCE(i.prior_taxa_identified, 0) AS prior_taxa_identified,
        COALESCE(i.prior_ids_given_improving_rate, 0) AS prior_ids_given_improving_rate,
        COALESCE(i.prior_ids_given_agree_rate, 0) AS prior_ids_given_agree_rate,
        COALESCE(i.prior_ids_given_disagree_rate, 0) AS prior_ids_given_disagree_rate,
        COALESCE(i.prior_ids_vision_rate, 0) AS prior_ids_vision_rate,

        COALESCE(i.reciprocity_ratio, 0) AS reciprocity_ratio,

     --Taxon features (fixed lookup)
    --t.taxon_rg_rate_shrunk,
    t.global_rg_rate,
    t.rank_level,
    t.taxon_popularity_rank,
    t.rg_rate_prior_source,
    t.taxon_cold_start,
    t.genus_popularity_rank,
    t.genus_rg_rate,
    t.family_rg_rate,
    t.taxon_avg_ids_to_rg,


     --Fixed Taxon confusion stats
    IFNULL(c.has_similar_species, FALSE)    AS has_similar_species,
    COALESCE(c.neighborhood_difficulty_dist_weighted,0)   AS neighborhood_difficulty_dist_weighted,
    COALESCE(c.neighborhood_difficulty_inv_dist, 0) AS neighborhood_difficulty_inv_dist,

    c.similar_species_count,

    --c.nbor_obs_count_sum,
    c.nbor_obs_count_mean,
    --c.nbor_obs_count_std,
    --c.nbor_obs_count_max,

    c.nbor_rg_rate_mean,
    c.nbor_rg_rate_std,
    --c.nbor_rg_rate_min,

    --c.weighted_mean_neighbor_rg_rate,
    c.nbor_rg_rate_inv_dist_weighted,


    --c.nbor_dist_max,
    c.nbor_dist_mean,

    c.rg_rate_vs_neighbors,

    c.nbor_count_same_genus,
    c.nbor_count_cross_genus,
    c.nbor_count_cross_family,

    c.cross_genus_confusion_rate,
    c.max_confusion_boundary_crossed,

    c.rg_percentile_in_neighborhood,
    c.rg_percentile_dist_weighted,

    c.neighbor_genus_diversity,
    --c.neighbor_rank_min,

    c.magnet_score,



    -- Taxa confusion graph
    cc.clustering_coefficient,

    dh.double_hop_nbrhd_size,
    dh.double_hop_nbrhd_genus,
    dh.double_hop_nbrhd_family,
    dh.genus_crossover_count,
    dh.family_crossover_count,

FROM features.base b
JOIN features.splits                     s  ON b.observation_id = s.observation_id
LEFT JOIN features.observations          ob ON b.observation_id = ob.observation_id
LEFT JOIN features.observers_entropy     oe ON b.observation_id = oe.observation_id
LEFT JOIN features.label                 l  ON b.observation_id = l.observation_id
JOIN features.identifications            i  ON b.observation_id = i.observation_id
LEFT JOIN features.taxon                 t  ON b.observation_id = t.observation_id
LEFT JOIN features.taxa_confusion        c  ON b.taxon_id = c.taxon_id
LEFT JOIN staged.users                   u  ON b.user_id = u.user_id

LEFT JOIN graph.clustering_coefficient   cc ON b.taxon_id = cc.taxon_id
LEFT JOIN graph.double_hop_stats         dh ON b.taxon_id = dh.taxon_id


WHERE l.label IS NOT NULL
ORDER BY b.observation_id;
