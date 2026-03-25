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
)    active_weeks
FROM staged.histogram_observed
