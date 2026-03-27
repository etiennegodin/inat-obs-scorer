-- features/identifications_at_window.sql
--
-- Point-in-time identification dynamics features.
-- Anchored at created_at +  — the inference moment.
--
-- Parameter : score_window_days  INT  (e.g. 7)
--
-- Requires  : staged.identifications
--             staged.observations
--             research_grade_windowed(eval_interval) macro

CREATE OR REPLACE TABLE features.identifications_at_window AS
WITH params AS (

    SELECT

    to_days(:score_window_days) AS score_window_days,

),

window_ids AS (

    SELECT
        i.observation_id,
        i.user_id,
        i.created_at,
        i.category,
        i.taxon_id,

        -- Sequence within window, oldest ID first — used to isolate first-ID signal
        ROW_NUMBER() OVER (
            PARTITION BY i.observation_id
            ORDER BY i.created_at
        ) AS id_seq

    FROM staged.identifications i
    CROSS JOIN params p
    JOIN staged.observations     o ON i.observation_id = o.id

    WHERE i."current" IS TRUE
      AND i.own_observation IS FALSE                              -- external IDs only
      AND i.created_at <= o.created_at + p.score_window_days

),

first_id AS (

    -- First external ID within the score window
    SELECT
        observation_id,
        created_at  AS first_id_at,
        taxon_id AS first_id_taxon_id,
        category    AS first_id_category
    FROM window_ids
    WHERE id_seq = 1

),

aggregates AS (

    SELECT
        observation_id,
        COUNT(*)                                            AS id_count_at_window,
        COUNT(DISTINCT user_id)                             AS id_diversity_at_window,
        COUNT(*) FILTER (WHERE category = 'supporting')    AS id_agree_count,
        COUNT(*) FILTER (WHERE category = 'improving')     AS id_improving_count,
        COUNT(*) FILTER (WHERE category = 'leading')       AS id_leading_count,
        COUNT(*) FILTER (WHERE category = 'maverick')      AS id_maverick_count

    FROM window_ids
    GROUP BY observation_id

),

-- Community taxon state as-of score_window.
-- Reuses the same macro that drives the label — anchored at score_window,
-- consensus_level_rg = TRUE means community reached species-level agreement within window.
community_state AS (

    SELECT
        observation_id,
        community_taxon_id IS NOT NULL                      AS has_community_taxon_at_window,
        consensus_level_rg                                  AS community_consensus_at_window,
        -- NULL-safe equality: handles observations with no community taxon yet
    -- sqlfluff:off
        community_taxon_id IS NOT DISTINCT FROM taxon_id   AS community_matches_submitted_at_window
    FROM research_grade_windowed((SELECT score_window_days FROM params))
    -- sqlfluff:on

)

SELECT
    o.id        AS observation_id,

    -- ── Volume ───────────────────────────────────────────────────────────────
    COALESCE(a.id_count_at_window,      0)                  AS id_count_at_window,
    COALESCE(a.id_diversity_at_window,  0)                  AS id_diversity_at_window,

    -- ── Velocity ─────────────────────────────────────────────────────────────
    -- IDs per day over the score window. Denominator is fixed (score_window_days)
    -- rather than actual elapsed time — at inference time all scored observations
    -- are exactly score_window_days old, so this is the correct denominator.
    ROUND(
        COALESCE(a.id_count_at_window, 0)::FLOAT / :score_window_days,
    4)                                                      AS id_velocity,


    -- ── Time-to-first-ID ─────────────────────────────────────────────────────
    -- NULL when no external ID arrived within the window. The model should treat
    -- NULL (no ID) vs 0.0 (immediate) as distinct states — use a separate binary.
    CASE
        WHEN f.first_id_at IS NOT NULL
        THEN date_part('day', f.first_id_at - o.created_at)
        ELSE NULL
    END                                                     AS time_to_first_id_days,

    CASE
        WHEN f.first_id_at IS NOT NULL
        THEN date_part('hour', f.first_id_at - o.created_at)
        ELSE NULL
    END                                                     AS time_to_first_id_hours,

    -- ── First-ID signals ─────────────────────────────────────────────────────
    f.first_id_at IS NOT NULL                               AS has_any_id,
    -- NULL when has_any_id is FALSE — model sees no-ID as its own state,
    -- not as disagreement
    CASE
        WHEN f.first_id_at IS NOT NULL
        THEN f.first_id_category = 'supporting'
        ELSE NULL
    END                                                     AS first_id_agrees,

    f.first_id_taxon_id = o.taxon_id AS first_id_agree_taxon,


       -- ── Agreement dynamics ───────────────────────────────────────────────────
    COALESCE(
        a.id_agree_count::FLOAT / NULLIF(a.id_count_at_window, 0), 0
    )                                                       AS pct_ids_agree_at_window,

    -- Improving + leading: IDs that refine or move beyond prior IDs.
    -- Distinct from pure agreement — signals active identification work.
    COALESCE(
        (a.id_improving_count + a.id_leading_count)::FLOAT
            / NULLIF(a.id_count_at_window, 0), 0
    )                                                       AS pct_ids_refining_at_window,

    COALESCE(a.id_maverick_count,    0)                     AS id_maverick_count_at_window,
    COALESCE(
        a.id_maverick_count::FLOAT / NULLIF(a.id_count_at_window, 0), 0
    )                                                       AS pct_ids_maverick_at_window,



    -- ── Community taxon state ─────────────────────────────────────────────────
    -- These three capture where the community *is* at score_window.
    -- community_consensus = species-level agreement reached (RG-eligible by taxon).
    -- community_matches_submitted = community agrees with observer's original ID.
    -- Together they stratify: no consensus / consensus but disagrees / full agreement.
    COALESCE(cs.has_community_taxon_at_window,         FALSE) AS has_community_taxon_at_window,
    COALESCE(cs.community_consensus_at_window,         FALSE) AS community_consensus_at_window,
    COALESCE(cs.community_matches_submitted_at_window, FALSE) AS community_matches_submitted_at_window

FROM staged.observations o
LEFT JOIN aggregates    a  ON a.observation_id = o.id
LEFT JOIN first_id      f  ON f.observation_id = o.id
LEFT JOIN community_state cs ON cs.observation_id = o.id
;
