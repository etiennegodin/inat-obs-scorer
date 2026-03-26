CREATE OR REPLACE MACRO blended_histogram(histo_col, alpha) AS TABLE

    WITH prior_vals AS (
        SELECT
            taxon_id,
            map_values(histo_col) AS val_list,
            list_sum(map_values(histo_col)) AS sum_val  -- sum, not max (see #2)
        FROM staged.histogram_scraped
    ),

    prior_proportions AS (
        SELECT
            *,
            list_transform(val_list, x -> x / sum_val) AS proportion
        FROM prior_vals
    ),

    prior_base AS (
        SELECT
            *,
            list_transform(proportion, x -> x * alpha) AS pseudo_counts
        FROM prior_proportions
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

    blended_raw AS (
        SELECT
            taxon_id,

            -- Element-wise: local + alpha * na_proportion
            list_transform(
                list_zip(local_histo, pseudo_counts),
                x -> x[1] + x[2]
            ) AS blended_raw,
        FROM histograms

    ),

    blended_normalized AS (
        SELECT
            *,
            list_sum(blended_raw) AS blended_total,
            list_max(blended_raw) AS max_val
        FROM blended_raw
    ),

    blended AS (
        SELECT
            *,
            list_transform(blended_raw, x -> x / blended_total) AS blended_pmf,
            0.8 * max_val AS threshold,
            list_sum(list_transform(
                generate_series(1, len(blended_raw)),
                i -> i * blended_raw[i]   -- no -1 needed
            )) / blended_total AS mu

        FROM blended_normalized
    ),

    moments AS (
        SELECT
            *,
            list_sum(list_transform(
                generate_series(1, len(blended_raw)),
                i -> blended_raw[i] * pow(i - mu, 2)  -- note: i not i-1, see below
            )) / blended_total AS variance,
            list_sum(list_transform(
                generate_series(1, len(blended_raw)),
                i -> blended_raw[i] * pow(i - mu, 4)
            )) / blended_total AS fourth_moment
        FROM blended
    ),

    kurtosis AS (
        SELECT
            *,
            (fourth_moment / pow(variance, 2)) - 3 AS excess_kurtosis
        FROM moments
    )

    SELECT
        *,

        -- Stats
        list_position(
            blended_raw,
            max_val
        ) AS peak_week,

        CEIL(peak_week / 4.0)::INT AS peak_month,

        list_count(
            list_filter(
                blended_raw,
                x -> x >= threshold
            )
        ) AS active_weeks

    FROM kurtosis
