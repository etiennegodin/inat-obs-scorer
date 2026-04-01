CREATE OR REPLACE TABLE features.training AS
SELECT
    --Identity
    m.observation_id,
    s.split,

    --Label
    l.label,

    -- for cv splits
    m.created_at, -- for cv splits

    -- Initial submisison
    m.taxon_id,
    m.init_rank_level,
    m.init_rank_level <= 10 AS obs_has_species_self_id,

    --Documentation features (submission-time safe)
    m.photo_count,
    m.has_description,
    --m.has_tags,
    --m.tag_count,
    m.positional_accuracy_m,
    m.geoprivacy IS NOT NULL AS geoprivacy_set,
    COALESCE(m.oauth_application_id, 0) AS oauth_application_id,

    --Temporal features
    m.obs_to_submit_lag_days,

    m.observed_week_sin,
    m.observed_week_cos,
    m.submitted_week_sin,
    m.submitted_week_cos,

    --Observer features
    --Temporal
    date_part('day', ob.observer_tenure) AS obv_tenure_days,
    ob.is_veteran AS obv_is_veteran,
    --date_part('day', ob.lag_since_last_obs) AS obv_lag_days_since_last_post,

    -- Observations
    LOG(ob.observer_obs_count_at_t + 1) AS obv_obs_count_log,
    ob.observer_rg_rate_at_t AS obv_rg_rate_lifetime,
    ob.observer_reputation_diff AS obv_reputation_diff,
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
    --ob.has_orcid AS obv_has_orcid,

    -- Role stats from identifications history
    -- Observer as an identifier features

    COALESCE(i.prior_ids_received, 0) AS id_prior_ids_received,
    COALESCE(i.prior_identifier_diversity, 0) AS id_prior_identifier_diversity,
    COALESCE(i.prior_observer_rg_rate, 0) AS id_prior_observer_rg_rate,
    COALESCE(i.prior_ids_given, 0) AS id_prior_ids_given,
    COALESCE(i.prior_taxa_identified, 0) AS id_prior_taxa_identified,
    --COALESCE(i.prior_ids_vision_rate, 0) AS id_prior_ids_vision_rate,
    COALESCE(i.reciprocity_ratio, 0) AS id_reciprocity_ratio,

    --Taxon time windowed features
    t.taxon_popularity_rank AS tx_popularity_rank,
    t.rg_rate_prior_source AS tx_rg_rate_prior_source,
    t.taxon_cold_start AS tx_cold_start,
    t.genus_popularity_rank AS tx_genus_popularity_rank,
    t.taxon_rg_rate_shrunk AS tx_taxon_rg_rate,
    t.genus_rg_rate AS tx_genus_rg_rate,
    t.family_rg_rate AS tx_family_rg_rate,
    t.order_rg_rate AS order_rg_rate,

    -- time
    t.effective_time_to_rg_mean, --fallback
    t.effective_time_to_rg_std,  --fallback
    --t.taxon_time_to_rg_mean,
    t.genus_time_to_rg_mean,
    t.family_time_to_rg_mean,
    --t.taxon_time_to_rg_std,
    t.genus_time_to_rg_std,
    t.family_time_to_rg_std,

    t.taxon_avg_ids_to_rg AS tx_avg_ids_to_rg,
    date_part('day', ob.lag_since_last_obs) - t.taxon_lag_days_median AS lag_diff,
    t.taxon_lag_days_median,
    t.taxon_lag_days_mean,
    t.taxon_lag_days_max,
    t.genus_lag_days_median,
    t.genus_lag_days_mean,
    t.genus_lag_days_max,
    t.family_lag_days_median,
    t.family_lag_days_mean,
    t.family_lag_days_max,

    -- Taxon specialist
    ts.mean_identifier_entropy,
    ts.pct_pure_specialists,
    ts.pct_specialists,
    ts.pct_generalist,
    ts.total_taxon_identifications,
    ts.identifier_count,

    -- Confusion graph topolgy stats (static)
    ct.similar_species_count IS NOT NULL AS has_similar_species, -- boolean flag
    ct.similar_species_count,
    --ct.nbor_dist_max,
    ct.nbor_dist_mean,
    ct.nbor_dist_median,
    ct.single_hop_genus_diversity,
    ct.single_hop_family_diversity,
    ct.max_confusion_boundary_crossed,
    ct.cross_genus_confusion_rate,
    ct.clustering_coefficient,
    ct.single_hop_rank_max,
    ct.single_hop_rank_min,
    ct.nbor_count_same_genus,
    ct.nbor_count_cross_genus,
    ct.nbor_count_cross_family,

    -- Dynamic confusion stats
    tc.rg_percentile_in_neighborhood,
    tc.rg_rate_vs_neighbors,
    tc.rg_percentile_dist_weighted,
    tc.neighborhood_difficulty_inv_dist,
    tc.neighborhood_difficulty_dist_weighted,
    tc.nbor_rg_rate_inv_dist_weighted,
    tc.weighted_mean_neighbor_rg_rate,
    tc.nbor_rg_rate_mean,
    tc.nbor_rg_rate_std,
    tc.nbor_rg_rate_min,
    tc.nbor_obs_count_mean,
    tc.nbor_obs_count_std,

    --Confusion graph topology
    dh.double_hop_nbrhd_size,
    dh.double_hop_nbrhd_genus_count,
    dh.double_hop_nbrhd_family_count,
    dh.genus_crossover_count,
    dh.family_crossover_count,
    dh.double_hop_genus_crossover_rate,
    dh.double_hop_max_boundary_crossed,
    dh.confusion_expansion_rate,
    dh.hidden_confusion_rate,

    -- Taxon observation and submission distributions
    tp.submission_pressure,
    tp.activity_at_pheno,
    tp.months_from_peak_pheno,
    tp.pheno_season_width,
    tp.observed_kurtosis,

    th.confusion_nbrhd_pheno_activity,
    th.focal_vs_nbrhd_pheno_ratio,

FROM features.model_population m
JOIN features.splits s ON m.observation_id = s.observation_id
LEFT JOIN features.observations ob ON m.observation_id = ob.observation_id
LEFT JOIN features.observers_entropy oe ON m.observation_id = oe.observation_id
LEFT JOIN features.label l ON m.observation_id = l.observation_id
JOIN features.identifications i ON m.observation_id = i.observation_id
LEFT JOIN features.taxon t ON m.observation_id = t.observation_id
LEFT JOIN features.taxon_specialist ts ON m.taxon_id = ts.taxon_id
LEFT JOIN features.taxa_confusion tc ON m.taxon_id = tc.taxon_id
LEFT JOIN graph.confusion_topology ct ON m.taxon_id = ct.taxon_id
LEFT JOIN graph.double_hop_derived dh ON m.taxon_id = dh.taxon_id
LEFT JOIN features.temporal tp ON m.observation_id = tp.observation_id
LEFT JOIN features.taxon_histo_conf th ON m.observation_id = th.observation_id

WHERE
    l.label IS NOT NULL
ORDER BY m.observation_id;
