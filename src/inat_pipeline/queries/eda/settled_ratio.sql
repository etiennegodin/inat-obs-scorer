SELECT
AVG(cumulative_settled_events / cumulative_events) as ratio_mean,
STDDEV(cumulative_settled_events / cumulative_events) as ratio_std
FROM graph.user_role_timeline
