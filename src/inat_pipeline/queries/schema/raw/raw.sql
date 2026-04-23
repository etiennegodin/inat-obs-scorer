CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.inat_api (
    raw_id VARCHAR PRIMARY KEY,
    raw_json JSON,
    scraped_at VARCHAR,
    scrapper_version VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.api_similar_species (
    raw_id VARCHAR PRIMARY KEY,
    raw_json JSON,
    scraped_at VARCHAR,
    scrapper_version VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.obs_histogram_na_observed (
    raw_id VARCHAR PRIMARY KEY,
    raw_json JSON,
    scraped_at VARCHAR,
    scrapper_version VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.obs_histogram_na_created (
    raw_id VARCHAR PRIMARY KEY,
    raw_json JSON,
    scraped_at VARCHAR,
    scrapper_version VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.downloads (
    id UBIGINT PRIMARY KEY,
    user_id UBIGINT,
    observed_on DATE,
    observed_on_string VARCHAR,
    created_at VARCHAR,
    quality_grade VARCHAR,
    tag_list VARCHAR,
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
    taxon_id UBIGINT
);

CREATE TABLE IF NOT EXISTS raw.taxa (
    id UBIGINT PRIMARY KEY,
    phylum VARCHAR,
    class VARCHAR,
    "order" VARCHAR,
    family VARCHAR,
    genus VARCHAR,
    specificEpithet VARCHAR,
    taxonRank VARCHAR,
    kingdom VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.places (
    id UBIGINT PRIMARY KEY,
    name VARCHAR,
    display_name VARCHAR,
    admin_level INTEGER,
    place_type INTEGER
);

CREATE TABLE IF NOT EXISTS raw.obs_sample (
    obs_count UBIGINT,
    id UBIGINT PRIMARY KEY,
    user_id UBIGINT,
    observed_on DATE,
    observed_on_string VARCHAR,
    created_at VARCHAR,
    quality_grade VARCHAR,
    tag_list VARCHAR,
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
    taxon_id UBIGINT
);
