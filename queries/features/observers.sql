CREATE OR REPLACE TABLE features.observers AS

SELECT o.user_id,

-- Volume & tenure
current_date as computed_as_of,
COUNT(DISTINCT(o.id)) as obs_count_total,
COUNT(DISTINCT(o.id)) FILTER (
    WHERE o.observed_on >= now() - INTERVAL '12 months'
    ) as obs_count_last_12m,
current_date - u.created_at as tenure_days,
CASE WHEN tenure_days > INTERVAL '730 days' THEN TRUE ELSE FALSE END AS is_veteran,
CASE WHEN u.orcid IS NOT NULL THEN TRUE ELSE FALSE END AS has_orcid,

-- Research Grade rate (core of observer reputation score)
COUNT(DISTINCT(o.id)) FILTER (
    WHERE o.quality_grade = 'research'
    ) AS rg_count,
rg_count / obs_count_total AS rg_rate_lifetime,
COUNT(DISTINCT(o.id)) FILTER (
    WHERE o.quality_grade = 'research' 
    AND o.observed_on >= now() - INTERVAL '12 months'
    ) AS rg_count_rolling,
rg_count_rolling / obs_count_last_12m AS rg_rate_last_12m,

-- Observer reputation score (v0.2 definition)
--expected_rg_rate                FLOAT,      -- from taxon/region baseline (joined from taxon_features)
--observer_reputation_raw         FLOAT,      -- rg_rate_lifetime / expected_rg_rate
--observer_reputation_score       FLOAT,      -- normalized 0-100

-- Taxonomic behaviour
COUNT(DISTINCT(o.order_id)) FILTER (WHERE o.order_id > 0) as taxon_diversity_order,
COUNT(DISTINCT(o.family_id)) FILTER (WHERE o.family_id > 0) as taxon_diversity_family,
COUNT(DISTINCT(o.genus_id)) FILTER (WHERE o.genus_id > 0) as taxon_diversity_genus,
COUNT(DISTINCT(o.species_id)) FILTER (WHERE o.species_id > 0) as taxon_diversity_species,

-- Documentation habits
AVG(LENGTH(o.observation_photos)) as avg_photo_count,
COUNT(DISTINCT(o.id)) FILTER ( WHERE o.description IS NOT NULL ) / obs_count_total AS pct_obs_with_description,
COUNT(DISTINCT(o.id)) FILTER ( WHERE o.license IS NOT NULL ) / obs_count_total AS pct_obs_with_license,
COUNT(DISTINCT(o.id)) FILTER (
    WHERE o.oauth_application_id = 3 
    OR o.oauth_application_id = 4
    ) / obs_count_total AS pct_obs_from_mobile,




FROM staged.observations o 
JOIN staged.users u ON u.user_id = o.user_id
LEFT JOIN staged.identifications i on o.user_id = i.user_id
GROUP BY o.user_id, u.created_at, u.orcid


--572540