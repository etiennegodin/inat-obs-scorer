CREATE OR REPLACE TABLE features.temporal AS

WITH observed_histo AS (

    SELECT * FROM blended_histogram(week_map_observed, 100)
),

created_histo AS (
    SELECT * FROM blended_histogram(week_map_created, 100)
),

divergence_base AS (
    SELECT
        ot.taxon_id,
        list_transform(ot.blended_raw, x -> x / ot.blended_total) AS P,
        list_transform(ct.blended_raw, x -> x / ct.blended_total) AS Q
    FROM observed_histo ot
    JOIN created_histo ct ON ot.taxon_id = ct.taxon_id
),

divergence AS (

-- probability distribution P
-- reference distribution Q

    SELECT
        taxon_id,
        P,
        Q,
        -- KL divergence
        list_sum(
            list_transform(
                generate_series(1, len(P)),
                i -> CASE
                    WHEN P[i] < 1e-10 THEN 0.0   -- 0 * log(0) = 0 by convention
                    ELSE P[i] * ln((P[i] + 1e-10) / (Q[i] + 1e-10))
                END
            )
        ) as KL_divergence,

        array_cosine_similarity(P::FLOAT [53], Q::FLOAT [53]) AS cosine_sim,
        array_distance(P::FLOAT [53], Q::FLOAT [53]) AS l2_distance

    FROM divergence_base

),

peaks_lag AS (
    SELECT
        ot.taxon_id,
        ((ct.peak_week - ot.peak_week) % 52 + 52) % 52 AS raw_diff,
        CASE
            WHEN raw_diff > 26 THEN raw_diff - 52  -- fold back to negative
            ELSE raw_diff
        END AS peak_lag_weeks
    FROM observed_histo ot
    JOIN created_histo ct ON ot.taxon_id = ct.taxon_id
)

SELECT

    o.id AS observation_id,
    o.taxon_id,
    o.created_at,
    o.observed_on,
    ct.blended_pmf as c_blended_pmf,
    ot.blended_pmf as o_blended_pmf,

    -- Observation stats
    ct.blended_pmf[LEAST(WEEK(o.created_at), 52)] AS submission_pressure,
    ot.blended_pmf[LEAST(WEEK(o.observed_on), 52)] AS activity_at_pheno,
    ot.blended_pmf[LEAST(WEEK(o.observed_on), 52)] AS activity_obs_pmf,

    LEAST(
        ABS(MONTH(o.created_at) - ct.peak_month),
        12 - ABS(MONTH(o.created_at) - ct.peak_month)
    ) AS months_from_peak_upload,

    LEAST(
        ABS(MONTH(o.observed_on) - ot.peak_month),
        12 - ABS(MONTH(o.observed_on) - ot.peak_month)
    ) AS months_from_peak_pheno,

    -- Distribution stats
    ot.active_weeks AS pheno_season_width,
    ct.active_weeks AS upload_season_width,

    -- Peaks lag
    p.peak_lag_weeks,

    -- 'Sharpness' of curves    ot.excess_kurtosis AS observed_kurtosis,
    ot.excess_kurtosis AS observed_kurtosis,
    ct.excess_kurtosis AS created_kurtosis,

    -- Distributions divergence
    d.KL_divergence,
    d.cosine_sim,
    d.l2_distance

FROM staged.observations o
JOIN observed_histo ot ON o.taxon_id = ot.taxon_id
JOIN created_histo ct ON o.taxon_id = ct.taxon_id
JOIN divergence d ON o.taxon_id = d.taxon_id
JOIN peaks_lag p ON o.taxon_id = p.taxon_id
