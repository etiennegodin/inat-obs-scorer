CREATE OR REPLACE TABLE features.temporal AS

WITH observed_stats AS(

    SELECT taxon_id,
    map_values(week_map) as val_list,
    list_max(map_values(week_map)) as max_val,
    0.8 * max_val AS threshold,
    list_position(
        val_list,          -- values list
        max_val) AS peak_week,
    FLOOR(peak_week / 4)::INT + 1 AS peak_month,
    list_unique(
        list_transform(
            list_filter(
                map_entries(week_map),
                x -> x.value >= threshold
            ),
            x -> x.key
        )
    ) as active_weeks
    FROM staged.histogram_observed
),

created_at_stats AS(

    SELECT taxon_id,
    map_values(week_map) as val_list,
    list_max(map_values(week_map)) as max_val,
    0.8 * max_val AS threshold,
    list_position(
        val_list,          -- values list
        max_val) AS peak_week,
    FLOOR(peak_week / 4)::INT + 1 AS peak_month,
    list_unique(
        list_transform(
            list_filter(
                map_entries(week_map),
                x -> x.value >= threshold
            ),
            x -> x.key
        )
    ) as active_weeks
    FROM staged.histogram_created
),

divergence AS(

-- probability distribution P
-- reference distribution Q


SELECT
ot.taxon_id,
list_transform(ot.val_list, x -> x / list_sum(ot.val_list)) AS P, -- normalized
list_transform(ct.val_list, x -> x / list_sum(ct.val_list)) AS Q, -- normalized

    -- KL divergence

list_sum(
    list_transform(
        generate_series(1,len(P)),
        i -> P[i] * ln(P[i] / (Q[i] + CAST('1.00E-010' AS FLOAT)) + 1 )
    )
) as KL_divergence,

array_cosine_similarity(P::FLOAT[53], Q::FLOAT[53]) AS cosine_sim,
array_distance(P::FLOAT[53], Q::FLOAT[53]) AS l2_distance

FROM observed_stats                 ot
JOIN created_at_stats               ct ON ot.taxon_id = ct.taxon_id


)

SELECT

    o.id AS observation_id,
    ot.active_weeks AS pheno_season_width,
    ct.active_weeks AS upload_season_width,


    -- Distance from peaks
    least(
        ABS(MONTH(o.created_at) - ct.peak_month),
        12 - ABS(MONTH(o.created_at) - ct.peak_month)
    )  AS months_from_peak_upload,

    least(
        ABS(MONTH(o.observed_on) - ot.peak_month),
        12 - ABS(MONTH(o.observed_on) - ot.peak_month)
    ) AS months_from_peak_pheno,

    -- Observation's relative place in histograms
        -- Submission curve
        ct.val_list[WEEK(o.created_at)] / ct.max_val as submission_pressure,

        -- Phenology curve
        ot.val_list[WEEK(o.observed_on)] / ot.max_val as activity_at_pheno,

    -- Distributions differences
    ct.peak_week - ot.peak_week AS peak_lag_weeks,

    d.KL_divergence,
    d.cosine_sim,
    d.l2_distance

FROM staged.observations            o
JOIN observed_stats                 ot ON o.taxon_id = ot.taxon_id
JOIN created_at_stats               ct ON o.taxon_id = ct.taxon_id
JOIN divergence                     d  ON o.taxon_id = d.taxon_id
