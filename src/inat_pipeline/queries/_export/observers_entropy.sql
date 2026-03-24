CREATE OR REPLACE TABLE features.observers_entropy AS

WITH base_obs AS (
    -- Same base as features.observers — just pull what entropy needs
    SELECT
        o.id        AS observation_id,
        o.user_id,
        o.created_at,
        o.species
    FROM staged.observations o
),

species_histograms AS (
    SELECT
        observation_id,
        user_id,
        created_at,

        -- Running species distribution up to (not including) this observation
        HISTOGRAM(species) FILTER (WHERE species IS NOT NULL) OVER observer_history
            AS species_hist,

        COUNT(*) OVER observer_history
            AS total_obs

    FROM base_obs

    WINDOW observer_history AS (
        PARTITION BY user_id
        ORDER BY created_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )
),

entropy AS (
    SELECT
        observation_id,
        user_id,
        total_obs,
        cardinality(species_hist)    AS distinct_species_count,

        list_reduce(
            list_transform(
                map_values(species_hist),
                c -> -(c::FLOAT / total_obs) * log2(c::FLOAT / total_obs)
            ),
            (a,b) -> a + b
        )  AS observer_species_entropy,
        CASE
            WHEN distinct_species_count > 1
            THEN observer_species_entropy / log2(distinct_species_count)
            ELSE 0
        END AS observer_species_entropy_norm
    FROM species_histograms
    WHERE total_obs > 0  -- no entropy on first observation
)

SELECT * FROM entropy;
