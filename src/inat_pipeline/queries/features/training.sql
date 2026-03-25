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
    b.tag_count,
    b.positional_accuracy_m,
    b.geoprivacy IS NOT NULL AS geoprivacy_set,
    COALESCE(b.oauth_application_id, 0) AS oauth_application_id,

    --Temporal features
    b.created_at,
    date_part('day', b.obs_to_submit_lag_days) AS obs_to_submit_lag_days,
    b.observed_week_sin,
    b.observed_week_cos,
    b.submitted_week_sin,
    b.submitted_week_cos,

    --Observer features
    --Temporal
    date_part('day', ob.observer_tenure) AS obv_tenure_days,
    ob.is_veteran AS obv_is_veteran,
    date_part('day', ob.lag_since_last_obs) AS obv_lag_days_since_last_post,

    -- Observations
    LOG(ob.observer_obs_count_at_t + 1) AS obv_obs_count_log,
    ob.observer_rg_rate_at_t AS obv_rg_rate_lifetime,
    ob.observer_reputation_diff AS obv_reputation_score,
    ob.observer_reputation_rank AS obv_reputation_rank,
    ob.rg_rate_is_reliable AS obv_rg_rate_is_reliable,

    --Taxonomic
    oe.observer_species_entropy_norm AS obv_tx_entropy,
    ob.observer_taxon_rg_rate_shrunk_at_t AS obv_tx_rg_rate,
    ob.observer_taxon_focus_rate AS obv_tx_focus_rate,

    -- Documentation Metadata
    ob.avg_photo_count AS obv_avg_photo_count,
    ob.pct_obs_with_description AS obv_pct_obs_with_description,
    ob.pct_obs_with_license AS obv_pct_obs_with_license,
    ob.pct_obs_from_mobile AS obv_pct_obs_from_mobile,
    ob.has_orcid AS obv_has_orcid,

    -- Role stats from identifications history
    -- Observer as an identifier features

    COALESCE(i.prior_ids_received, 0) AS id_prior_ids_received,
    COALESCE(i.prior_identifier_diversity, 0) AS id_prior_identifier_diversity,
    COALESCE(i.prior_observer_rg_rate, 0) AS id_prior_observer_rg_rate,
    COALESCE(i.prior_ids_given, 0) AS id_prior_ids_given,
    COALESCE(i.prior_taxa_identified, 0) AS id_prior_taxa_identified,
    COALESCE(i.prior_ids_vision_rate, 0) AS id_prior_ids_vision_rate,
    COALESCE(i.reciprocity_ratio, 0) AS id_reciprocity_ratio,

    -- Identification dynamics at score_window
    -- Volume & velocity
    iw.id_count_at_window,
    iw.id_diversity_at_window,
    iw.id_velocity,

    -- Time-to-first-ID (NULL = no ID arrived; keep NULL, do not COALESCE to 0
    -- — the model should distinguish "no ID" from "immediate ID")
    iw.time_to_first_id_days,

    -- First-ID signals
    iw.has_any_id,
    --iw.first_id_agrees, leak?

    -- Agreement dynamics
    --iw.pct_ids_agree_at_window, leak?

    -- Community taxon state at score_window
    iw.has_community_taxon_at_window,
    iw.community_consensus_at_window,
    iw.community_matches_submitted_at_window,

    --Taxon features
    t.taxon_popularity_rank AS tx_popularity_rank,
    t.rg_rate_prior_source AS tx_rg_rate_prior_source,
    t.taxon_cold_start AS tx_cold_start,
    t.genus_popularity_rank AS tx_genus_popularity_rank,
    COALESCE(t.genus_rg_rate, 0) AS tx_genus_rg_rate,
    COALESCE(t.family_rg_rate, 0) AS tx_family_rg_rate,
    --t.taxon_avg_ids_to_rg AS tx_avg_ids_to_rg,

    -- Taxon confusion stats (static)
    IFNULL(c.has_similar_species, FALSE) AS tx_conf_has_similar,
    COALESCE(c.neighborhood_difficulty_dist_weighted, 0) AS tx_conf_nbrhd_diff_dist_weighted,
    COALESCE(c.neighborhood_difficulty_inv_dist, 0) AS tx_conf_nbrhd_diff_inv_dist,
    c.similar_species_count AS tx_conf_similar_species_count,
    c.nbor_obs_count_mean AS tx_conf_nbrhd_obs_count_mean,
    c.nbor_obs_count_std AS tx_conf_nbrhd_obs_count_std,
    c.nbor_rg_rate_mean AS tx_conf_nbrhd_rg_rate_mean,
    c.nbor_rg_rate_std AS tx_conf_nbrhd_rg_rate_std,
    c.nbor_rg_rate_inv_dist_weighted AS tx_conf_nbrhd_rg_rate_inv_dist_weighted,
    c.nbor_dist_mean AS tx_conf_nbrhd_dist_mean,
    c.rg_rate_vs_neighbors AS tx_conf_rate_vs_neighbors,
    c.nbor_count_same_genus AS tx_conf_nbrhd_same_genus,
    c.nbor_count_cross_genus AS tx_conf_nbrhd_cross_genus,
    c.nbor_count_cross_family AS tx_conf_nbrhd_cross_family,
    c.cross_genus_confusion_rate AS tx_conf_cross_genus_conf_rate,
    c.max_confusion_boundary_crossed AS tx_conf_max_conf_boundary_crossed,
    c.rg_percentile_in_neighborhood AS tx_conf_rg_perc_in_nbrhd,
    c.rg_percentile_dist_weighted AS tx_conf_rg_perc_dist_weighted,
    c.neighbor_genus_diversity AS tx_conf_nbrhd_genus_div,
    c.neighbor_rank_min AS tx_conf__rank_min,
    c.magnet_score AS tx_confusion_magnet_score,

    --Confusion graph topology
    cc.clustering_coefficient AS tx_conf_2hop_clustering_coefficient,
    dh.double_hop_nbrhd_size AS tx_conf_2hop_size,
    dh.double_hop_nbrhd_genus AS tx_conf_2hop_genus,
    dh.double_hop_nbrhd_family AS tx_conf_2hop_family,
    dh.genus_crossover_count AS tx_conf_genus_crossovers,
    dh.family_crossover_count AS tx_conf_family_crossover,

FROM features.base b
JOIN features.splits s ON b.observation_id = s.observation_id
LEFT JOIN features.observations ob ON b.observation_id = ob.observation_id
LEFT JOIN features.observers_entropy oe ON b.observation_id = oe.observation_id
LEFT JOIN features.label l ON b.observation_id = l.observation_id
JOIN features.identifications i ON b.observation_id = i.observation_id
LEFT JOIN features.identifications_at_window iw ON b.observation_id = iw.observation_id
LEFT JOIN features.taxon t ON b.observation_id = t.observation_id
LEFT JOIN features.taxa_confusion c ON b.taxon_id = c.taxon_id
LEFT JOIN graph.clustering_coefficient cc ON b.taxon_id = cc.taxon_id
LEFT JOIN graph.double_hop_stats dh ON b.taxon_id = dh.taxon_id

WHERE
    l.label IS NOT NULL
    AND has_any_id IS TRUE
ORDER BY b.observation_id;
