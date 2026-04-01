CREATE OR REPLACE MACRO research_grade_windowed(eval_interval) AS TABLE

    WITH base_obs AS (

        SELECT
            c.observation_id,
            o.user_id,
            c.community_taxon,
            c.taxon_id,
            c.consensus_level_rg,
            c.score,
            c.rank_level,
            c.n_ids_at_window,
            c.created_at,
            c.observed_on,
            c.identification_id_ct,
            c.identifier_ct,
            c.id_created_at,

            -- verifiable label
            CASE
                WHEN o.created_at IS NULL THEN FALSE
                WHEN o.latitude IS NULL THEN FALSE
                WHEN LENGTH(o.observation_photos) = 0 THEN FALSE
                WHEN o.captive_cultivated = TRUE THEN FALSE
                ELSE TRUE

            END AS verifiable,

            -- research grade label
            CASE
                WHEN verifiable IS FALSE THEN FALSE
                -- Eligible but consensus didn't reach species level
                -- Community taxon doesn't match submitted taxon
                WHEN c.community_taxon != o.taxon_id THEN FALSE
                WHEN c.consensus_level_rg IS FALSE THEN FALSE
                -- Eligible, species consensus, but no confirming identifiers
                WHEN n_ids_at_window = 0 THEN FALSE
                ELSE TRUE
            END AS is_rg,

        FROM staged.observations o
        LEFT JOIN community_taxon_windowed(eval_interval) c ON o.id = c.observation_id
        WHERE o.created_at IS NOT NULL
    ),

    time_to_rg AS (

        SELECT
            observation_id,
            date_part('day', id_created_at - created_at) AS time_to_rg_days,

        FROM base_obs
        WHERE is_rg
    )

    SELECT
        b.*,
        t.time_to_rg_days

    FROM base_obs b
    LEFT JOIN time_to_rg t ON t.observation_id = b.observation_id
