CREATE OR REPLACE MACRO blended_histogram(histo_col, alpha) AS TABLE

    WITH prior_base AS (

        SELECT
            taxon_id,
            map_values(histo_col) as val_list,
            list_max(val_list) as max_val,
            list_transform(val_list, x -> x / max_val) AS proportion,
            list_transform(proportion, x -> x * alpha) AS pseudo_counts, -- for shrinkage
            list_sum(val_list) as sum_val,

        FROM staged.histogram_scraped
    ),

    local_base AS (

        SELECT
            taxon_id,
            histo_col AS local_histo
        FROM staged.histogram_local
    ),

    histograms AS (

        SELECT
            p.*,
            l.local_histo

        FROM prior_base p
        JOIN local_base l ON p.taxon_id = l.taxon_id

    ),

    blended AS (

        SELECT
            taxon_id,

            -- Element-wise: local + alpha * na_proportion
            list_transform(
                list_zip(local_histo, pseudo_counts),
                x -> x[1] + x[2]
            ) AS blended_raw,

            -- Sum for normalization
            list_sum(
                list_transform(
                    list_zip(local_histo, pseudo_counts),
                    x -> x[1] + x[2]
                )
            ) AS blended_total,

            list_transform(
                blended_raw,
                x -> x / blended_total
            ) AS blended_normalised,

            list_max(blended_raw) as max_val,
            0.8 * max_val AS threshold,

            -- new mean
            list_sum(list_transform(generate_series(1, len(blended_raw)), i -> i * blended_raw[i])) / blended_total AS mu,

        FROM histograms
    ),

    moments AS (

        SELECT
            *,
            -- 3. Calculate Variance (2nd Moment)
            list_sum(list_transform(generate_series(1, len(blended_raw)), i -> blended_raw[i] * pow(i - mu, 2))) / blended_total AS variance,
            -- 4. Calculate 4th Moment
            list_sum(list_transform(generate_series(1, len(blended_raw)), i -> blended_raw[i] * pow(i - mu, 4))) / blended_total AS fourth_moment
        FROM blended
    ),

    kurtosis AS (

        SELECT
            taxon_id,
            (fourth_moment / pow(variance, 2)) - 3 AS kurtosis,

        FROM moments

    )

    SELECT
        b.taxon_id,
        k.kurtosis,

        -- Stats
        list_position(
            b.blended_raw,
            b.max_val
        ) AS peak_week,

        FLOOR(peak_week / 4)::INT + 1 AS peak_month,
        list_count(
            list_filter(
                blended_raw,
                x -> x >= threshold
            )
        ) AS active_weeks

    FROM blended b
    JOIN kurtosis k on b.taxon_id = k.taxon_id
