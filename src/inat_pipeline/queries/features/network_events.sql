CREATE OR REPLACE TABLE graph.network_events AS
SELECT
    *,
    -- first time this specific counterpart appeared for this user+role
    CASE WHEN ROW_NUMBER() OVER (
        PARTITION BY user_id, role, counterpart_id
        ORDER BY created_at
    ) = 1 THEN 1 ELSE 0 END AS is_new_counterpart,

    -- first time this taxon appeared for this user+role
    CASE WHEN ROW_NUMBER() OVER (
        PARTITION BY user_id, role, taxon_id
        ORDER BY created_at
    ) = 1 THEN 1 ELSE 0 END AS is_new_taxon

FROM graph.network_events
