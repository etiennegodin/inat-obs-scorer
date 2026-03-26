CREATE OR REPLACE MACRO histogram_stats(histo_col, alpha) AS TABLE

    WITH histo_stats AS (

        SELECT
            taxon_id,
            map_values(histo_col) as val_list,
            list_max(val_list) as max_val,
            list_transform(val_list, x -> x / max_val) AS proportion,
            list_transform(proportion, x -> x * alpha) AS pseudo_counts,

            list_sum(val_list) as sum_val,
            list_sum(list_transform(generate_series(1, len(val_list)), i -> i * val_list[i])) / sum_val AS mu,
            0.8 * max_val AS threshold,
            list_position(
                val_list,          -- values list
                max_val
            ) AS peak_week,
            FLOOR(peak_week / 4)::INT + 1 AS peak_month,
            list_unique(
                list_transform(
                    list_filter(
                        map_entries(histo_col),
                        x -> x.value >= threshold
                    ),
                    x -> x.key
                )
            ) as active_weeks
        FROM staged.histogram_scraped
    ),

    moments AS (

        SELECT
            *,
            -- 3. Calculate Variance (2nd Moment)
            list_sum(list_transform(generate_series(1, len(val_list)), i -> val_list[i] * pow(i - mu, 2))) / sum_val AS variance,
            -- 4. Calculate 4th Moment
            list_sum(list_transform(generate_series(1, len(val_list)), i -> val_list[i] * pow(i - mu, 4))) / sum_val AS fourth_moment
        FROM histo_stats
    ),

    kurtosis AS (

        SELECT
            taxon_id,
            (fourth_moment / pow(variance, 2)) - 3 AS kurtosis,

        FROM moments

    )

    SELECT
        s.*,
        k.kurtosis

    FROM histo_stats s
    JOIN kurtosis k on s.taxon_id = k.taxon_id
