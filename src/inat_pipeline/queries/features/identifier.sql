CREATE OR REPLACE TABLE features.identifiers_score AS

WITH base_id AS(
    SELECT
        i.id                      AS identification_id,
        i.user_id,
        i.taxon_id,
        i.created_at,
        i.category,
        i.current,
        i.own_observation,
        i.vision,
        i."order",
        i.family,
        i.genus,
        i.species,
        u.created_at              AS user_created_at,
        u.identifier_only,
        u.identifier_only,
        rg.is_rg,

    FROM staged.identifications i
    JOIN staged.users u
        ON i.user_id = u.user_id
    LEFT JOIN research_grade_windowed(INTERVAL '999 years') rg
        ON rg.observation_id = i.observation_id
    WHERE own_observation IS FALSE
),

aggregates AS(
    SELECT
        -- Keys
        identification_id,
        user_id,
        taxon_id,
        created_at,

        -- Temporal & status
        created_at - user_created_at     AS identifier_tenure,
        CASE WHEN identifier_tenure > INTERVAL '730 days' THEN TRUE ELSE FALSE END AS is_veteran,

        -- All-time identifier history (excluding current observation)
        COALESCE(COUNT(*)           OVER identifier_history, 0) AS identifier_id_count_at_t,
        COALESCE(SUM(is_rg::INT)    OVER identifier_history, 0) AS identifier_rg_count_at_t,
        COALESCE(identifier_rg_count_at_t::FLOAT
                / NULLIF(identifier_id_count_at_t, 0), 0)      AS identifier_rg_rate_at_t,


        -- This taxon's rg rate from this identifier's history
        COALESCE(COUNT(*)           OVER identifier_taxon_history, 0)  AS identifier_taxon_id_count_at_t,
        COALESCE(SUM(is_rg::INT)    OVER identifier_taxon_history, 0) AS identifier_taxon_id_rg_count_at_t,
        COALESCE(identifier_taxon_id_count_at_t::FLOAT
                / NULLIF(identifier_taxon_id_rg_count_at_t, 0), 0)      AS identifier_taxon_rg_rate_at_t,

        identifier_taxon_id_count_at_t::FLOAT
            / NULLIF(identifier_id_count_at_t, 0) AS identifier_taxon_focus_rate,


        -- identifier last
        created_at - LAG (created_at, 1, NULL ) OVER identifier_history AS lag_since_last_id,

        -- Taxonomic behaviour
        COUNT(DISTINCT("order")) FILTER (WHERE "order" IS NOT NULL) OVER identifier_history AS taxon_diversity_order,
        COUNT(DISTINCT(family)) FILTER (WHERE family IS NOT NULL) OVER identifier_history AS taxon_diversity_family,
        COUNT(DISTINCT(genus)) FILTER (WHERE genus IS NOT NULL) OVER identifier_history AS taxon_diversity_genus,
        COALESCE(COUNT(DISTINCT(species)) FILTER (WHERE species IS NOT NULL) OVER identifier_history,0) AS taxon_diversity_species,

        -- Documentation quality
        COALESCE(AVG(vision::INT) OVER identifier_history,0) AS avg_vision,
        COALESCE(COUNT(*) FILTER (WHERE category = 'supporting') OVER identifier_history,0)  AS n_id_supporting,
        COALESCE(COUNT(*) FILTER (WHERE category = 'improving') OVER identifier_history,0)  AS n_id_improving,
        COALESCE(COUNT(*)  FILTER (WHERE category = 'maverick') OVER identifier_history,0) AS n_id_maverick,
        COALESCE(COUNT(*)  FILTER (WHERE category = 'leading')  OVER identifier_history,0)AS n_id_leading,

    FROM base_id

    WINDOW
        identifier_history AS (
            PARTITION BY user_id
            ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ),
        identifier_taxon_history AS (
            PARTITION BY user_id, taxon_id
            ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )
),

ranked AS (
    SELECT
        *,
        -- leaky but approximation point-in-time rank among identifiers active in same period
        PERCENT_RANK() OVER (
            PARTITION BY DATE_TRUNC('month', created_at)
            ORDER BY identifier_rg_rate_at_t
            ) AS identifier_rg_rate_rank,

        -- how much does this id have more ids than others around same time
        PERCENT_RANK() OVER (
            PARTITION BY DATE_TRUNC('month', created_at)
            ORDER BY identifier_taxon_rg_rate_at_t
            ) AS identifier_taxon_rg_rate_rank,


        FROM aggregates
)

SELECT * FROM ranked;
