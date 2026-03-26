/*
CREATE OR REPLACE TABLE features.temporal AS

divergence AS (

-- probability distribution P
-- reference distribution Q

    SELECT
        ot.taxon_id,
        list_transform(ot.val_list, x -> x / ot.sum_val) AS P, -- normalized
        list_transform(ct.val_list, x -> x / ct.sum_val) AS Q, -- normalized

        -- KL divergence

        list_sum(
            list_transform(
                generate_series(1, len(P)),
                i -> P[i] * ln(P[i] / (Q[i] + CAST('1.00E-010' AS FLOAT)) + 1)
            )
        ) as KL_divergence,

        array_cosine_similarity(P::FLOAT [53], Q::FLOAT [53]) AS cosine_sim,
        array_distance(P::FLOAT [53], Q::FLOAT [53]) AS l2_distance

    FROM observed_stats ot
    JOIN created_at_stats ct ON ot.taxon_id = ct.taxon_id

),

peaks_lag AS (

    SELECT
        ot.taxon_id,
        (ct.peak_week - ot.peak_week) % 52 AS diff,
        CASE
            WHEN diff > 26 THEN -diff
            ELSE diff
        END AS peak_lag_weeks,

    FROM observed_stats ot
    JOIN created_at_stats ct ON ot.taxon_id = ct.taxon_id
)

SELECT

    o.id AS observation_id,
    o.created_at,
    o.observed_on,

    -- Observation stats
    ct.val_list[WEEK(o.created_at) - 1] / ct.max_val as submission_pressure,

    ot.val_list[WEEK(o.observed_on) - 1] / ot.max_val as activity_at_pheno,
    ot.val_list[WEEK(o.observed_on) - 1] / ot.sum_val AS activity_obs_pmf,

    least(
        ABS(MONTH(o.created_at) - ct.peak_month),
        12 - ABS(MONTH(o.created_at) - ct.peak_month)
    ) AS months_from_peak_upload,

    least(
        ABS(MONTH(o.observed_on) - ot.peak_month),
        12 - ABS(MONTH(o.observed_on) - ot.peak_month)
    ) AS months_from_peak_pheno,

    -- Distribution stats
    ot.active_weeks AS pheno_season_width,
    ct.active_weeks AS upload_season_width,

    -- Peaks lag
    p.peak_lag_weeks,

    -- 'Sharpness' of curves
    k.observed_kurtosis,
    k.created_kurtosis,

    -- Distributions divergence
    d.KL_divergence,
    d.cosine_sim,
    d.l2_distance

FROM staged.observations o
JOIN observed_stats ot ON o.taxon_id = ot.taxon_id
JOIN created_at_stats ct ON o.taxon_id = ct.taxon_id
JOIN divergence d ON o.taxon_id = d.taxon_id
JOIN kurtosis k ON o.taxon_id = k.taxon_id
JOIN peaks_lag p ON o.taxon_id = p.taxon_id
*/
