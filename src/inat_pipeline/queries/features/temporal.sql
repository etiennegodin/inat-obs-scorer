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

    -- KL divergence
    /*
    list_sum(
        list_transform(
            generate_series(1,len(ot.val_list)),
            i -> ot.val_list[i] * ln(ot.val_list[i] / ct.val_list[i])
        )
    ) as KL_divergence,
    */


FROM staged.observations            o
JOIN observed_stats                 ot ON o.taxon_id = ot.taxon_id
JOIN created_at_stats               ct ON o.taxon_id = ct.taxon_id
