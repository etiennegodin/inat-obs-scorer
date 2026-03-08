CREATE OR REPLACE TABLE features.observers AS


SELECT

-- Keys
    o.id AS observation_id,
    o.user_id,
    o.taxon_id,
    o.created_at,
    o.quality_grade,

-- Temporal & status
o.created_at - u.created_at AS observer_tenure_days,
CASE WHEN observer_tenure_days > INTERVAL '730 days' THEN TRUE ELSE FALSE END AS is_veteran,
CASE WHEN u.orcid IS NOT NULL THEN TRUE ELSE FALSE END AS has_orcid,

COALESCE(
    research_grade_windowed(INTERVAL '999 years'),
    0
    ) AS observer_rg_count_at_t,

-- Observer stats at time T (excluding current observation)
COALESCE(
    COUNT(*) OVER observer_history, 0
    ) AS observer_obs_count_at_t,

COALESCE(
    AVG(CASE WHEN o.quality_grade = 'research' THEN 1.0 ELSE 0 END) OVER observer_history, 0
    ) AS observer_rg_rate_at_t,

observer_obs_count_at_t >= 20 AS rg_rate_is_reliable,

-- Observer 12months stats at time T 
COALESCE(
    COUNT(*) OVER observer_12m, 0
    ) AS observer_obs_count_12m,

COALESCE(
    SUM(CASE WHEN o.quality_grade = 'research' THEN 1.0 ELSE 0 END) OVER observer_12m, 0
    )
    AS observer_rg_count_12m,

COALESCE(
    SUM(CASE WHEN quality_grade = 'research' THEN 1.0 ELSE 0 END) OVER observer_12m
    / NULLIF(COUNT(*) OVER observer_12m, 0), 0
    )AS observer_rg_rate_12m,


-- Observer reputation score (v0.2 definition)
t.taxon_rg_rate as expected_rg_rate,
observer_rg_rate_at_t / expected_rg_rate as observer_reputation_raw,
-- (observer_reputation_raw - MIN(observer_reputation_raw) OVER ()) * 1.0 / NULLIF(MAX(observer_reputation_raw) OVER() - MIN(observer_reputation_raw) OVER (),0 ) AS observer_reputation_score,

-- Taxonomic behaviour
COUNT(DISTINCT(o.order)) FILTER (WHERE o.order IS NOT NULL) OVER observer_history AS taxon_diversity_order,
COUNT(DISTINCT(o.family)) FILTER (WHERE o.family IS NOT NULL) OVER observer_history AS taxon_diversity_family,
COUNT(DISTINCT(o.genus)) FILTER (WHERE o.genus IS NOT NULL) OVER observer_history AS taxon_diversity_genus,
COALESCE(COUNT(DISTINCT(o.species)) FILTER (WHERE o.species IS NOT NULL) OVER observer_history,0) AS taxon_diversity_species,

-- Documentation records 
AVG(LENGTH(o.observation_photos)) OVER observer_history AS avg_photo_count,
COUNT(DISTINCT(o.id)) FILTER (
    WHERE o.description IS NOT NULL
    ) OVER observer_history / observer_obs_count_at_t AS pct_obs_with_description,
COUNT(DISTINCT(o.id)) FILTER (
    WHERE o.license IS NOT NULL
    ) OVER observer_history / observer_obs_count_at_t  AS pct_obs_with_license,
COUNT(DISTINCT(o.id)) FILTER (
    WHERE o.oauth_application_id = 3 
    OR o.oauth_application_id = 4
    ) OVER observer_history / observer_obs_count_at_t   AS pct_obs_from_mobile,


FROM staged.observations o
JOIN staged.users u ON o.user_id = u.user_id
JOIN features.taxon t on t.observation_id = o.id

WINDOW
    observer_history AS (
        PARTITION BY o.user_id
        ORDER BY o.created_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ),
    observer_12m AS (
        PARTITION BY o.user_id
        ORDER BY o.created_at
        RANGE BETWEEN INTERVAL 12 MONTHS PRECEDING AND INTERVAL 1 MICROSECOND PRECEDING
    ),
    taxon_history AS (
    PARTITION BY taxon_id
    ORDER BY created_at
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )