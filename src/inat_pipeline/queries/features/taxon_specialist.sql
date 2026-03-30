CREATE OR REPLACE TABLE features.taxon_specialist AS

WITH raw_counts AS (
    -- 1. Get raw counts per user and family
    SELECT
        i.user_id,
        t.family_id,
        COUNT(*) AS family_count
    FROM staged.identifications i
    JOIN staged.taxa t ON i.taxon_id = t.taxon_id
    WHERE
        i.own_observation IS FALSE
        AND i.created_at < :cutoff_date
    GROUP BY ALL
),

probabilities AS (
    -- 2. Calculate fractions and total_ids
    -- FILTER: Only keep users with > 5 total observations
    SELECT
        user_id,
        family_count,
        SUM(family_count) OVER (PARTITION BY user_id) AS total_ids,
        (family_count * 1.0 / SUM(family_count) OVER (PARTITION BY user_id)) AS p
    FROM raw_counts
    QUALIFY total_ids > 5 -- DuckDB specific: filters window results immediately
),

identifier_entropy AS (
    -- 3. Compute Entropy and assign a "Label" to each user
    SELECT
        user_id,
        -SUM(p * LN(p + 1e-10)) AS entropy,
        CASE
            WHEN -SUM(p * LN(p + 1e-10)) <= 0.0001 THEN 'pure_specialist'
            WHEN -SUM(p * LN(p + 1e-10)) < 0.5 THEN 'specialist'
            WHEN -SUM(p * LN(p + 1e-10)) < 2.0 THEN 'generalist'
            ELSE 'broad_generalist'
        END AS user_category
    FROM probabilities
    GROUP BY user_id
),

taxon_specialist_signal AS (
    -- 4. Aggregate user metrics up to the Taxon level
    SELECT
        i.taxon_id,
        t.family_id,
        AVG(ie.entropy) AS mean_identifier_entropy,
        -- Distribution of user types identifying this taxon
        COUNT(CASE WHEN ie.user_category = 'pure_specialist' THEN 1 END) * 1.0 / COUNT(*) AS pct_pure_specialists,
        -- Pct of identifiers who are "specialist" types (entropy < 1.1)
        COUNT(CASE WHEN ie.user_category = 'specialist' THEN 1 END) * 1.0 / COUNT(*) AS pct_specialists,
        COUNT(CASE WHEN ie.user_category = 'generalist' THEN 1 END) * 1.0 / COUNT(*) AS pct_generalist,
        COUNT(i.user_id) AS total_taxon_identifications,
        COUNT(DISTINCT i.user_id) AS identifier_count
    FROM staged.identifications i
    JOIN staged.taxa t ON i.taxon_id = t.taxon_id
    JOIN identifier_entropy ie ON i.user_id = ie.user_id
    WHERE i.own_observation IS FALSE
    GROUP BY i.taxon_id, t.family_id
    HAVING total_taxon_identifications >= 6
)

SELECT * FROM taxon_specialist_signal;
