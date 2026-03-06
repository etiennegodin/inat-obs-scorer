CREATE OR REPLACE TABLE features.training AS
SELECT
    -- Identity
    o.observation_id,
    o.user_id,
    --o.split,

    -- Label
    o.label,

    -- Documentation features (submission-time safe)
    o.photo_count,
    o.has_description,
    o.has_tags,
    o.tag_count,
    o.has_license,
    o.positional_accuracy_m,
    o.obscured,
    o.geoprivacy IS NOT NULL                AS geoprivacy_set,
    o.captive,                              -- should be FALSE for all training rows
    o.oauth_application_id,

    -- Temporal features
    o.obs_to_submit_lag_days,
    o.observed_month,
    o.observed_week,
    o.observed_hour,
    o.submitted_hour,
    o.submitted_year,

    -- Observer features (from observer_features, computed pre-split)
    ob.tenure_days AS obv_tenure_days,
    ob.obs_count_total AS obv_obs_count_total,
    ob.rg_rate_lifetime AS obv_rg_rate_lifetime,
    ob.rg_rate_last_12m AS obv_rg_rate_last_12m,
    ob.rg_rate_is_reliable AS obv_rg_rate_is_reliable,
    ob.observer_reputation_score AS obv_reputation_score,
    ob.taxon_diversity_family AS obv_taxon_diversity_family,
    ob.taxon_diversity_genus AS obv_taxon_diversity_genus,
    ob.taxon_diversity_species AS obv_taxon_diversity_species,
    ob.avg_photo_count AS obv_avg_photo_count,
    ob.pct_obs_with_description AS obv_pct_obs_with_description,
    ob.pct_obs_with_license AS obv_pct_obs_with_license,
    ob.pct_obs_from_mobile AS obv_pct_obs_from_mobile,
    ob.has_orcid AS obv_has_orcid,
    ob.sampling_pool AS obv_sampling_pool,


    id.ids_given_count,


    -- Taxon features (fixed lookup)
    t.taxon_rg_rate,
    t.taxon_popularity_rank,
    --t.is_difficult_group,
    --t.iconic_taxon_name,
    --t.taxon_avg_ids_to_rg,
    t.taxonRank

FROM features.observations o
LEFT JOIN features.observers ob ON o.user_id = ob.user_id
LEFT JOIN features.identifications i ON o.observation_id = i.observation_id
LEFT JOIN features.identifiers id ON o.user_id = id.user_id
LEFT JOIN features.taxon t ON o.taxon_id = t.taxon_id
WHERE o.label IS NOT NULL