CREATE OR REPLACE TABLE features.training AS
SELECT
    -- Identity
    s.split,

    -- Label
    o.label,

    -- Documentation features (submission-time safe)
    o.photo_count,
    o.has_description,
    o.has_tags,
    o.tag_count,
    --o.has_license,
    --o.positional_accuracy_m,
    o.obscured,
    o.geoprivacy IS NOT NULL                AS geoprivacy_set,
    o.captive,                              -- should be FALSE for all training rows
    COALESCE(o.oauth_application_id,0)      AS oauth_application_id,

    -- Temporal features
    o.created_at,
    date_part('day',o.obs_to_submit_lag_days) AS obs_to_submit_lag_days,
    --o.observed_month,
    o.observed_week,
    o.observed_hour,
    o.submitted_hour,
    o.submitted_year,

    -- Observer features (from observer_features, computed at observation time)
        -- Temporal
        date_part('day',ob.observer_tenure) AS obv_tenure_days,
        ob.is_veteran AS obv_is_veteran,
        date_part('day',ob.lag_since_last_obs) AS obv_lag_days_since_last_post,

        -- Observations
        ob.observer_obs_count_at_t AS obv_obs_count_total,
        ob.observer_rg_rate_at_t AS obv_rg_rate_lifetime,
        ob.observer_reputation_raw AS obv_reputation_score,
        ob.observer_reputation_rank AS obv_reputation_rank,
        ob.rg_rate_is_reliable AS obv_rg_rate_is_reliable,

        -- Identifiers interactions
        ob.n_identifiers_mean_rank AS obv_n_identifiers_mean_rank,
        ob.n_identifiers_agree_mean_rank AS obv_n_identifiers_agree_mean_rank,

        -- Taxonomic
        ob.taxon_diversity_species AS obv_taxon_diversity_species,
        oe.observer_species_entropy_norm AS obv_taxon_entropy,

        -- Metadata
        ob.avg_photo_count AS obv_avg_photo_count,
        ob.pct_obs_with_description AS obv_pct_obs_with_description,
        ob.pct_obs_with_license AS obv_pct_obs_with_license,
        ob.pct_obs_from_mobile AS obv_pct_obs_from_mobile,
        ob.has_orcid AS obv_has_orcid,

        -- Identifications history

        -- Identifiers
        --ir.identifiers_total,
        ir.identifiers_agreeing,
        ir.identifiers_improving,
        ir.identifiers_maverick,
        ir.identifiers_vision,

    -- Taxon features (fixed lookup)
    t.taxon_rg_rate,
    t.rank_level,
    t.taxon_popularity_rank,
    t.rg_rate_source,
    t.taxon_avg_ids_to_rg,
    t.taxon_cold_start,
    --t.is_difficult_group,
    --t.iconic_taxon_name,

    -- Fixed Taxon confusion stats
    IFNULL(c.has_similar_species, FALSE)    AS has_similar_species,
    COALESCE(c.neighborhood_difficulty,0)   AS neighborhood_difficulty,
    c.rg_percentile_in_neighborhood,
    c.magnet_score,
    c.rg_rate_vs_neighbors,

FROM features.observations o
JOIN features.splits                     s  ON o.observation_id = s.observation_id
LEFT JOIN features.observers             ob ON o.observation_id = ob.observation_id
LEFT JOIN features.observers_entropy     oe ON o.observation_id = oe.observation_id

--LEFT JOIN features.identifications       i  ON o.observation_id = i.observation_id
LEFT JOIN features.identifiers           ir ON o.observation_id = ir.observation_id
LEFT JOIN features.taxon                 t  ON o.observation_id = t.observation_id
LEFT JOIN features.taxa_confusion        c  ON o.taxon_id = c.taxon_id
WHERE o.label IS NOT NULL
