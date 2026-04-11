CREATE SCHEMA IF NOT EXISTS tests;

CREATE TABLE IF NOT EXISTS tests.community_taxon (
    observation_id UBIGINT PRIMARY KEY,
    taxon_id UBIGINT,
    created_at TIMESTAMPTZ,
    observed_on DATE,
    community_taxon UBIGINT,
    rank_level INTEGER,
    score DOUBLE,
    cumulative UBIGINT,
    identification_id_ct UBIGINT,
    id_created_at TIMESTAMPTZ,
    identifier_ct UBIGINT,
    consensus_level_rg BOOLEAN
);

CREATE TABLE IF NOT EXISTS tests.community_taxon_comparison (
    observation_id UBIGINT,
    c_taxon_id UBIGINT,
    o_taxon_id UBIGINT,
    c_rank INTEGER,
    o_rank INTEGER,
    o_rg BOOLEAN,
    c_rg BOOLEAN,
    correct_taxon_level BOOLEAN,
    correct_taxon BOOLEAN
);

CREATE TABLE IF NOT EXISTS tests.reasearch_grade (
    observation_id UBIGINT,
    is_rg BOOLEAN,
    o_rg BOOLEAN
);
