CREATE SCHEMA IF NOT EXISTS staged;

CREATE TABLE IF NOT EXISTS staged.observations (
    id UBIGINT PRIMARY KEY,
    uuid VARCHAR,
    user_id UBIGINT,
    observed_on DATE,
    observed_on_string TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    quality_grade VARCHAR,
    tag_list VARCHAR [],
    description VARCHAR,
    license VARCHAR,
    captive_cultivated BOOLEAN,
    oauth_application_id UBIGINT,
    latitude DOUBLE,
    longitude DOUBLE,
    positional_accuracy DOUBLE,
    geoprivacy VARCHAR,
    place_guess VARCHAR,
    taxon_geoprivacy VARCHAR,
    coordinates_obscured BOOLEAN,
    positioning_method VARCHAR,
    taxon_id UBIGINT,
    phylum VARCHAR,
    class VARCHAR,
    "order" VARCHAR,
    family VARCHAR,
    genus VARCHAR,
    species VARCHAR,
    rank VARCHAR,
    rank_level INTEGER,
    owners_identification_from_vision BOOLEAN,
    identifications_count UBIGINT,
    comments_count UBIGINT,
    faves_count UBIGINT,
    community_taxon_id UBIGINT
);

CREATE TABLE IF NOT EXISTS staged.identifications (
    observation_id UBIGINT,
    id UBIGINT PRIMARY KEY,
    uuid VARCHAR,
    created_at TIMESTAMPTZ,
    user_id UBIGINT,
    user_login VARCHAR,
    user_observations_count UBIGINT,
    user_identifications_count UBIGINT,
    user_species_count UBIGINT,
    body VARCHAR,
    category VARCHAR,
    "current" BOOLEAN,
    own_observation BOOLEAN,
    vision BOOLEAN,
    disagreement BOOLEAN,
    previous_observation_taxon_id UBIGINT,
    taxon_id UBIGINT,
    phylum VARCHAR,
    class VARCHAR,
    "order" VARCHAR,
    family VARCHAR,
    genus VARCHAR,
    species VARCHAR,
    rank VARCHAR
);

CREATE TABLE IF NOT EXISTS staged.taxa (
    taxon_id UBIGINT PRIMARY KEY,
    rank VARCHAR,
    phylum VARCHAR,
    class VARCHAR,
    "order" VARCHAR,
    family VARCHAR,
    genus VARCHAR,
    species VARCHAR,
    phylum_id UBIGINT,
    class_id UBIGINT,
    order_id UBIGINT,
    family_id UBIGINT,
    genus_id UBIGINT,
    species_id UBIGINT,
    rank_level INTEGER
);

CREATE TABLE IF NOT EXISTS staged.users (
    user_id UBIGINT PRIMARY KEY,
    created_at TIMESTAMPTZ,
    orcid VARCHAR,
    observer BOOLEAN,
    identifier BOOLEAN,
    observer_only BOOLEAN,
    identifier_only BOOLEAN
);

CREATE TABLE IF NOT EXISTS staged.similar_species (
    taxon_id UBIGINT,
    counts UBIGINT,
    similar_taxon_id UBIGINT
);

CREATE TABLE IF NOT EXISTS staged.taxa_asymmetry (
    taxon_id UBIGINT PRIMARY KEY,
    out_degree UBIGINT,
    in_degree UBIGINT,
    magnet_score BIGINT
);

CREATE TABLE IF NOT EXISTS staged.taxa_distance (
    taxon_id UBIGINT,
    similar_taxon_id UBIGINT,
    taxonomic_distance INTEGER,
    taxonomic_distance_rank VARCHAR
);

CREATE TABLE IF NOT EXISTS staged.photos (
    observation_id UBIGINT,
    photo_id UBIGINT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS staged.places (
    place_id UBIGINT PRIMARY KEY,
    name VARCHAR,
    display_name VARCHAR,
    admin_level INTEGER,
    place_type INTEGER
);

CREATE TABLE IF NOT EXISTS staged.histogram_scraped (
    taxon_id UBIGINT PRIMARY KEY,
    week_map_observed MAP (INT, INT),
    week_map_created MAP (INT, INT)
);

CREATE TABLE IF NOT EXISTS staged.histogram_local (
    taxon_id UBIGINT PRIMARY KEY,
    week_map_created INT [],
    total_obs_created UBIGINT,
    week_map_observed INT [],
    total_obs_observed UBIGINT
);
