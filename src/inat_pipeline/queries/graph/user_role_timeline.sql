CREATE OR REPLACE TABLE graph.user_role_timeline AS
SELECT
    user_id,
    role,
    created_at,
    observation_id,

    -- volume
    SUM(1) OVER w                        AS cumulative_events,

    -- counterpart diversity (exact, via first-occurrence flag)
    SUM(is_new_counterpart) OVER w       AS cumulative_distinct_counterparts,

    -- taxa breadth (exact, same trick)
    SUM(is_new_taxon) OVER w             AS cumulative_distinct_taxa,

    COALESCE(SUM(1) FILTER (WHERE category = 'supporting') OVER w, 0) AS cumulative_supporting,
    COALESCE(SUM(1) FILTER (WHERE category = 'maverick') OVER w,0) AS cumulative_maverick,
    COALESCE(SUM(1) FILTER (WHERE category = 'leading') OVER w,0) AS cumulative_leading,
    COALESCE(SUM(1) FILTER (WHERE category = 'improving') OVER w,0) AS cumulative_improving,
    COALESCE(SUM(1) FILTER (WHERE vision IS TRUE ) OVER w,0) AS cumulative_vision,


    -- per-pair running edge weight (how many times has this pair interacted)
    SUM(1) OVER (
        PARTITION BY user_id, role, counterpart_id
        ORDER BY created_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                    AS running_pair_weight

FROM graph.network_events
WINDOW w AS (
    PARTITION BY user_id, role
    ORDER BY created_at
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
