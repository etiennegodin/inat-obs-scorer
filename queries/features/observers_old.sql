CREATE OR REPLACE TABLE features.observers AS

WITH user_taxon_exposure AS(
    SELECT
        o.user_id,
        AVG(t.taxon_rg_rate) AS expected_rg_rate  -- weighted by their actual obs distribution
    FROM staged.observations o
    JOIN features.taxon t ON o.taxon_id = t.taxon_id
    --WHERE o.created_at < :as_of_date
    GROUP BY o.user_id
)

SELECT o.id,
o.user_id,

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
obs_count_total AS obs_count_for_rg_rate,
obs_count_for_rg_rate >= 20 AS rg_rate_is_reliable,


-- Observer reputation score (v0.2 definition)
ut.expected_rg_rate as expected_rg_rate,
rg_rate_lifetime / expected_rg_rate as observer_reputation_raw,
(observer_reputation_raw - MIN(observer_reputation_raw) OVER ()) * 1.0 /
    NULLIF(MAX(observer_reputation_raw) OVER() - MIN(observer_reputation_raw) OVER (),0 ) AS observer_reputation_score,

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


mode(o.sampling_pool) AS sampling_pool,


FROM staged.observations o 
JOIN staged.users u ON u.user_id = o.user_id
JOIN user_taxon_exposure ut on o.user_id = ut.user_id 
GROUP BY o.user_id, u.created_at, u.orcid, ut.expected_rg_rate;
