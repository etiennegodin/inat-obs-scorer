CREATE OR REPLACE TABLE features.observers AS

WITH base_obs AS(
    SELECT
        o.id                      AS observation_id,
        o.user_id,
        o.taxon_id,
        o.created_at,
        o.description,
        o.license,
        o.oauth_application_id,
        o.observation_photos,
        o."order",
        o.family,
        o.genus,
        o.species,
        u.created_at              AS user_created_at,
        u.orcid,
        t.taxon_rg_rate           AS expected_rg_rate,
        rg.n_identifiers_at_window,
        rg.n_identifiers_agree_at_window,
        -- Honest RG label from macro (no leakage)
        COALESCE(rg.is_rg, FALSE)           AS is_rg

    FROM staged.observations o
    JOIN staged.users u
        ON o.user_id = u.user_id
    JOIN features.taxon t
        ON t.observation_id = o.id
    -- Unbounded window = all identifications ever, for current-state scoring
    JOIN staged.identifications i on o.observation_id = i.observation_id
    LEFT JOIN research_grade_windowed(INTERVAL '999 years') rg
        ON rg.observation_id = o.id
),

aggregates AS(
    SELECT
        -- Keys
        observation_id,
        user_id,
        taxon_id,
        created_at,

        -- Temporal & status
        created_at - user_created_at     AS observer_tenure,
        CASE WHEN observer_tenure > INTERVAL '730 days' THEN TRUE ELSE FALSE END AS is_veteran,
        orcid IS NOT NULL AS has_orcid,

        -- All-time observer history (excluding current observation)
        COALESCE(COUNT(*)           OVER observer_history, 0) AS observer_obs_count_at_t,
        COALESCE(SUM(is_rg::INT)    OVER observer_history, 0) AS observer_rg_count_at_t,
        COALESCE(observer_rg_count_at_t::FLOAT
                / NULLIF(observer_obs_count_at_t, 0), 0)          AS observer_rg_rate_at_t,

        observer_obs_count_at_t >= 20 AS rg_rate_is_reliable,

        -- Observer last
        created_at - LAG (created_at, 1, NULL ) OVER observer_history AS lag_since_last_obs,

        -- Observer reputation score (v0.2 definition)
        expected_rg_rate,
        COALESCE(observer_rg_rate_at_t /  NULLIF(expected_rg_rate, 0),0) AS observer_reputation_raw,
        -- (observer_reputation_raw - MIN(observer_reputation_raw) OVER ()) * 1.0 / NULLIF(MAX(observer_reputation_raw) OVER() - MIN(observer_reputation_raw) OVER (),0 ) AS observer_reputation_score,

        -- Taxonomic behaviour
        COUNT(DISTINCT("order")) FILTER (WHERE "order" IS NOT NULL) OVER observer_history AS taxon_diversity_order,
        COUNT(DISTINCT(family)) FILTER (WHERE family IS NOT NULL) OVER observer_history AS taxon_diversity_family,
        COUNT(DISTINCT(genus)) FILTER (WHERE genus IS NOT NULL) OVER observer_history AS taxon_diversity_genus,
        COALESCE(COUNT(DISTINCT(species)) FILTER (WHERE species IS NOT NULL) OVER observer_history,0) AS taxon_diversity_species,

        -- Community interaction ?
        -- logic, if lots of ids observers observation is either
        -- popular, easy,might be reviewed faster than avg
        AVG(n_identifiers_at_window) OVER observer_history AS n_identifiers_mean,
        AVG(n_identifiers_agree_at_window) OVER observer_history AS n_identifiers_agree_mean,

        -- Documentation quality
        COALESCE(
            AVG(LENGTH(observation_photos)) OVER observer_history,0) AS avg_photo_count,
        COALESCE(COUNT(DISTINCT(observation_id)) FILTER (
            WHERE description IS NOT NULL
            ) OVER observer_history / NULLIF(observer_obs_count_at_t,0),0) AS pct_obs_with_description,
        COALESCE(COUNT(DISTINCT(observation_id)) FILTER (
            WHERE license IS NOT NULL
            ) OVER observer_history / NULLIF(observer_obs_count_at_t,0),0)  AS pct_obs_with_license,
        COALESCE(COUNT(DISTINCT(observation_id)) FILTER (
            WHERE oauth_application_id = 3
            OR oauth_application_id = 4
            ) OVER observer_history / NULLIF(observer_obs_count_at_t,0),0)   AS pct_obs_from_mobile,


    FROM base_obs

    WINDOW
        observer_history AS (
            PARTITION BY user_id
            ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )
),

ranked AS (
    SELECT
        *,
        -- leaky but approximation point-in-time rank among observers active in same period
        PERCENT_RANK() OVER (
            PARTITION BY DATE_TRUNC('month', created_at)
            ORDER BY observer_reputation_raw
            ) AS observer_reputation_rank,

        -- how much does this obs have more ids than others around same time
        PERCENT_RANK() OVER (
            PARTITION BY DATE_TRUNC('month', created_at)
            ORDER BY n_identifiers_mean
            ) AS n_identifiers_mean_rank,

        PERCENT_RANK() OVER (
            PARTITION BY DATE_TRUNC('month', created_at)
                ORDER BY n_identifiers_agree_mean
                ) AS n_identifiers_agree_mean_rank,

        FROM aggregates
)

SELECT * FROM ranked;
