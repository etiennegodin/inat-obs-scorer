CREATE SCHEMA IF NOT EXISTS graph;

CREATE TABLE IF NOT EXISTS graph.network_events_raw (
    user_id UBIGINT,
    role VARCHAR,
    counterpart_id UBIGINT,
    taxon_id UBIGINT,
    created_at TIMESTAMPTZ,
    observation_id UBIGINT,
    vision BOOLEAN,
    category VARCHAR,
    is_rg BOOLEAN,
    outcome_settled INTEGER
);

CREATE TABLE IF NOT EXISTS graph.network_events (
    user_id UBIGINT,
    role VARCHAR,
    counterpart_id UBIGINT,
    taxon_id UBIGINT,
    created_at TIMESTAMPTZ,
    observation_id UBIGINT,
    vision BOOLEAN,
    category VARCHAR,
    is_rg BOOLEAN,
    outcome_settled INTEGER,
    is_new_counterpart INTEGER,
    is_new_taxon INTEGER
);

CREATE TABLE IF NOT EXISTS graph.user_role_timeline (
    user_id UBIGINT,
    role VARCHAR,
    created_at TIMESTAMPTZ,
    observation_id UBIGINT,
    cumulative_events UBIGINT,
    cumulative_distinct_counterparts UBIGINT,
    cumulative_distinct_taxa UBIGINT,
    cumulative_vision UBIGINT,
    cumulative_settled_events UBIGINT,
    cumulative_settled_rg UBIGINT,
    running_pair_weight UBIGINT
);

CREATE TABLE IF NOT EXISTS graph.observer_identifier_edges (
    observer_id UBIGINT,
    identifier_id UBIGINT,
    n_ids UBIGINT,
    n_obs_touched UBIGINT,
    identifier_effort_share DOUBLE,
    log_weight DOUBLE
);

CREATE TABLE IF NOT EXISTS graph.user_nodes (
    user_id UBIGINT,
    role VARCHAR
);

CREATE TABLE IF NOT EXISTS graph.confusion_topology (
    taxon_id UBIGINT PRIMARY KEY,
    magnet_score BIGINT,
    similar_species_count UBIGINT,
    nbor_dist_max INTEGER,
    nbor_dist_mean DOUBLE,
    nbor_dist_median DOUBLE,
    single_hop_genus_diversity UBIGINT,
    single_hop_family_diversity UBIGINT,
    single_hop_rank_median DOUBLE,
    single_hop_rank_max INTEGER,
    single_hop_rank_min INTEGER,
    nbor_count_same_genus UBIGINT,
    nbor_count_cross_genus UBIGINT,
    nbor_count_cross_family UBIGINT,
    max_confusion_boundary_crossed INTEGER,
    cross_genus_confusion_rate DOUBLE,
    clustering_coefficient DOUBLE
);

CREATE TABLE IF NOT EXISTS graph.double_hop (
    taxon_id UBIGINT PRIMARY KEY,
    double_hop_nbrhd_size UBIGINT,
    double_hop_nbrhd_genus_count UBIGINT,
    double_hop_nbrhd_family_count UBIGINT,
    genus_crossover_count UBIGINT,
    family_crossover_count UBIGINT,
    double_hop_genus_crossover_rate DOUBLE,
    double_hop_max_boundary_crossed INTEGER
);

CREATE TABLE IF NOT EXISTS graph.double_hop_derived (
    taxon_id UBIGINT PRIMARY KEY,
    double_hop_nbrhd_size UBIGINT,
    double_hop_nbrhd_genus_count UBIGINT,
    double_hop_nbrhd_family_count UBIGINT,
    genus_crossover_count UBIGINT,
    family_crossover_count UBIGINT,
    double_hop_genus_crossover_rate DOUBLE,
    double_hop_max_boundary_crossed INTEGER,
    confusion_expansion_rate DOUBLE,
    hidden_confusion_rate DOUBLE
);
