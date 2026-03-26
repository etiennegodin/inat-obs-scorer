-- created at week local histo
CREATE OR REPLACE MACRO histogram_local(col) AS TABLE

    WITH base AS (

        SELECT
            id AS observation_id,
            taxon_id,
            WEEK(col) AS week_id,
        FROM staged.observations
    ),

    -- 1. Create a reference of all possible weeks (0-51)
    week_spine AS (
        SELECT range AS week_id
        FROM range(1, 53)
    ),

    -- 2. Create a unique list of taxons
    taxon_list AS (
        SELECT DISTINCT taxon_id FROM base
    ),

    -- 3. Create the "Matrix" (Every taxon x Every week)
    taxon_week_matrix AS (
        SELECT
            t.taxon_id,
            w.week_id
        FROM taxon_list t
        CROSS JOIN week_spine w
    ),

    -- 3. Join and fill
    counts_filled AS (
        SELECT
            m.taxon_id,
            m.week_id,
            COUNT(b.observation_id) AS week_count
        FROM taxon_week_matrix m
        LEFT JOIN base b USING (taxon_id, week_id)
        GROUP BY ALL
    )

    SELECT
        taxon_id,
        -- If you want a full list of counts in order:
        list(week_count ORDER BY week_id) AS full_year_vector,
        sum(week_count) AS total_obs
    FROM counts_filled
    GROUP BY taxon_id;
