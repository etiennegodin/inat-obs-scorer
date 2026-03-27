CREATE OR REPLACE MACRO community_taxon_windowed(eval_interval) AS TABLE

    WITH id_window AS (
        SELECT
            i.observation_id,
            i.taxon_id AS id_taxon_id,
            i.user_id,
            i.id AS identification_id,
            i.own_observation,
            i.created_at,

            -- Full ancestor chain for each identification's taxon
            t.species_id,
            t.genus_id,
            t.family_id,
            t.order_id,
            t.class_id,
            t.phylum_id,
            t.rank_level,  -- species=10, genus=20, family=30, etc

        FROM staged.identifications i
        JOIN staged.observations o ON i.observation_id = o.id
        JOIN staged.taxa t ON i.taxon_id = t.taxon_id

        WHERE
            i."current" IS TRUE
            AND i.created_at >= o.created_at
            AND i.created_at <= o.created_at + eval_interval  -- <- window parameter
    ),

    -- All candidate taxa to score = every taxon appearing in any ID,
    -- plus all their ancestors
    candidates AS (

        SELECT DISTINCT
            observation_id,
            id_taxon_id AS candidate_taxon_id,
            rank_level
        FROM id_window

        UNION
        SELECT DISTINCT
            observation_id,
            genus_id,
            20
        FROM id_window
        WHERE genus_id IS NOT NULL
        UNION
        SELECT DISTINCT
            observation_id,
            family_id,
            30
        FROM id_window
        WHERE family_id IS NOT NULL
        UNION
        SELECT DISTINCT
            observation_id,
            order_id,
            40
        FROM id_window
        WHERE order_id IS NOT NULL
        UNION
        SELECT DISTINCT
            observation_id,
            class_id,
            50
        FROM id_window
        WHERE class_id IS NOT NULL

    ),

    scored_timeline AS (

        SELECT
            c.observation_id,
            c.candidate_taxon_id,
            c.rank_level,
            iw.identification_id,
            iw.created_at,
            iw.user_id,

            -- 1. Running count of IDs supporting this candidate (Cumulative)
            COUNT(*) FILTER (
                WHERE iw.id_taxon_id = c.candidate_taxon_id
                OR iw.species_id = c.candidate_taxon_id
                OR iw.genus_id = c.candidate_taxon_id
                OR iw.family_id = c.candidate_taxon_id
                OR iw.order_id = c.candidate_taxon_id
                OR iw.class_id = c.candidate_taxon_id
            ) OVER (
                PARTITION BY c.observation_id, c.candidate_taxon_id
                ORDER BY iw.created_at
            ) AS running_cumulative,

            -- 2. Running count of IDs ABOVE this candidate (Conservative/Ancestors)
            COUNT(*) FILTER (
                WHERE iw.rank_level > c.rank_level
                AND iw.id_taxon_id != c.candidate_taxon_id
            ) OVER (
                PARTITION BY c.observation_id, c.candidate_taxon_id
                ORDER BY iw.created_at
            ) AS running_ancestor_disagreements,

            -- 3. Total IDs submitted for this observation up to this point in time
            COUNT(*) OVER (
                PARTITION BY c.observation_id, c.candidate_taxon_id
                ORDER BY iw.created_at
            ) AS running_total_ids

        FROM candidates c
        JOIN id_window iw ON c.observation_id = iw.observation_id

    ),

    calculated_score AS (
        SELECT
            *,
            -- Disagreements = Total - (Supporters + Ancestor IDs)
            (running_total_ids - running_cumulative - running_ancestor_disagreements) AS running_disagreements,

            -- Score = Supporters / (Supporters + Disagreements + Ancestors)
            -- Note: In iNaturalist logic, the denominator is effectively the total IDs
            running_cumulative::FLOAT / NULLIF(running_cumulative + running_disagreements + running_ancestor_disagreements, 0) AS current_score

        FROM scored_timeline

    ),

    -- Pick lowest ranked (most specific) taxon with score > 2/3 and cumulative >= 2
    community_taxon AS (
        SELECT DISTINCT ON (observation_id)
            observation_id,
            candidate_taxon_id AS community_taxon,
            identification_id,
            user_id,
            rank_level,
            current_score AS score,
            running_cumulative AS cumulative,
            rank_level <= 10 AS consensus_level_rg,  -- species or below (subspecies = rank 5)

        FROM calculated_score
        WHERE
            current_score > 2 / 3.0
            AND running_cumulative >= 2
        ORDER BY observation_id, rank_level ASC, created_at ASC -- ASC = most specific first
    )

    SELECT
        o.id AS observation_id,
        o.taxon_id,
        o.created_at,
        ct.community_taxon,
        ct.rank_level,
        ct.score,
        ct.cumulative AS n_ids_at_window,
        ct.identification_id AS id_rg,
        ct.user_id AS id_user,
        COALESCE(ct.consensus_level_rg, FALSE) AS consensus_level_rg,
    -- RG requires community taxon to match submitted taxon

    FROM staged.observations o
    LEFT JOIN community_taxon ct ON o.id = ct.observation_id;
