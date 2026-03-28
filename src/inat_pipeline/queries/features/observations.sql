CREATE OR REPLACE TABLE features.observations AS
WITH config AS (
    SELECT to_days(:label_window_days) AS window_val

),

base_obs AS (
    SELECT
        o.id AS observation_id,
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
        u.created_at AS user_created_at,
        u.orcid,
        t.taxon_rg_rate_shrunk AS expected_rg_rate,
        rg.n_identifiers_at_window,
        rg.n_identifiers_agree_at_window,
        -- Honest RG label from macro (no leakage)
        COALESCE(rg.is_rg, FALSE) AS is_rg

    FROM staged.observations o
    JOIN staged.users u
        ON o.user_id = u.user_id
    JOIN features.taxon t
        ON t.taxon_id = o.taxon_id
    -- All identifications within score window
    LEFT JOIN research_grade_windowed((SELECT window_val FROM config)) rg
        ON rg.observation_id = o.id
),

aggregates AS (
    SELECT
        -- Keys
        observation_id,
        user_id,
        taxon_id,
        created_at,

        -- ── Static observer attributes ──────────────────────────────
        created_at - user_created_at AS observer_tenure,
        CASE WHEN observer_tenure > INTERVAL '730 days' THEN TRUE ELSE FALSE END AS is_veteran,
        orcid IS NOT NULL AS has_orcid,

        -- ── Own-observation history ──────────────────────────────────
        COALESCE(COUNT(*) OVER observer_history, 0) AS observer_obs_count_at_t,
        COALESCE(SUM(is_rg::INT) OVER observer_history, 0) AS observer_rg_count_at_t,

        -- Bayesian-shrunk RG rate (α=10, matches taxon shrinkage convention)
        expected_rg_rate,
        (COALESCE(SUM(is_rg::INT) OVER observer_history, 0) + 10 * expected_rg_rate)
        / NULLIF(COALESCE(COUNT(*) OVER observer_history, 0) + 10, 0)
            AS observer_rg_rate_at_t,

        COALESCE(observer_rg_rate_at_t / NULLIF(expected_rg_rate, 0), 0) AS observer_reputation_raw,
        COALESCE(NULLIF(observer_rg_rate_at_t, 0) - NULLIF(expected_rg_rate, 0), 0) AS observer_reputation_diff,

        observer_obs_count_at_t >= 20 AS rg_rate_is_reliable,

        COALESCE(COUNT(*) OVER observer_taxon_history, 0) AS observer_taxon_obs_count_at_t,
        COALESCE(SUM(is_rg::INT) OVER observer_taxon_history, 0) AS observer_taxon_obs_rg_count_at_t,

        -- To do shrinkage here at a = 15 hyperparam

        COALESCE(
            observer_taxon_obs_rg_count_at_t::FLOAT
            / NULLIF(observer_taxon_obs_count_at_t, 0), 0
        ) AS observer_taxon_rg_rate_raw_at_t,

        COALESCE(
            (15 * expected_rg_rate + observer_taxon_obs_rg_count_at_t)
            / (15 + observer_taxon_obs_count_at_t), 0
        ) AS observer_taxon_rg_rate_shrunk_at_t,

        observer_taxon_obs_count_at_t::FLOAT
        / NULLIF(observer_obs_count_at_t, 0) AS observer_taxon_focus_rate,

        -- ── Observation cadence ──────────────────────────────────────
        created_at - LAG(created_at, 1, NULL) OVER observer_history AS lag_since_last_obs,

        -- Observer reputation score (v0.2 definition)

        -- (observer_reputation_raw - MIN(observer_reputation_raw) OVER ()) * 1.0 / NULLIF(MAX(observer_reputation_raw) OVER() - MIN(observer_reputation_raw) OVER (),0 ) AS observer_reputation_score,

        -- Taxonomic behaviour
        COUNT(DISTINCT("order")) OVER observer_history AS taxon_diversity_order,
        COUNT(DISTINCT(family)) OVER observer_history AS taxon_diversity_family,
        COUNT(DISTINCT(genus)) OVER observer_history AS taxon_diversity_genus,

        -- ── Community engagement received ────────────────────────────
        AVG(n_identifiers_at_window) OVER observer_history AS n_identifiers_mean,
        AVG(n_identifiers_agree_at_window) OVER observer_history AS n_identifiers_agree_mean,

        -- Documentation quality
        COALESCE(
            AVG(LENGTH(observation_photos)) OVER observer_history, 0
        ) AS avg_photo_count,
        COALESCE(COUNT(DISTINCT(observation_id)) FILTER (
            WHERE description IS NOT NULL
        ) OVER observer_history / NULLIF(observer_obs_count_at_t, 0), 0) AS pct_obs_with_description,
        COALESCE(COUNT(DISTINCT(observation_id)) FILTER (
            WHERE license IS NOT NULL
        ) OVER observer_history / NULLIF(observer_obs_count_at_t, 0), 0) AS pct_obs_with_license,
        COALESCE(COUNT(DISTINCT(observation_id)) FILTER (
            WHERE oauth_application_id = 3
            OR oauth_application_id = 4
        ) OVER observer_history / NULLIF(observer_obs_count_at_t, 0), 0) AS pct_obs_from_mobile,

    FROM base_obs

    WINDOW
        observer_history AS (
            PARTITION BY user_id
            ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ),
        observer_taxon_history AS (
            PARTITION BY user_id, taxon_id
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

    FROM aggregates
)

SELECT * FROM ranked
