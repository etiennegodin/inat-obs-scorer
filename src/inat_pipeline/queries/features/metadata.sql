CREATE SCHEMA IF NOT EXISTS features;

CREATE OR REPLACE TABLE features.metadata AS

SELECT

-- Primary keys & joins
o.id AS observation_id,
o.uuid,
o.user.id AS user_id,

-- Label

-- Temporal (submission-time signals)
o.observed_on,
o.created_at,
o.created_at - o.observed_on AS obs_to_submit_lag_days,

YEAR(o.observed_on) AS observed_year,
--MONTH(o.observed_on) AS observed_month,
WEEK(o.observed_on) AS observed_week,
DAY(o.observed_on) AS observed_day,
COALESCE(HOUR(observed_on_string),0) AS observed_hour,

HOUR(o.created_at) AS submitted_hour,
DAY(o.created_at) AS submitted_day,
--MONTH(o.created_at) AS submitted_month,
WEEK(o.created_at) AS submitted_week,
YEAR(o.created_at) AS submitted_year,

-- Documentation quality (submission-time)
LENGTH(o.observation_photos) AS photo_count,
CASE
    WHEN o.description is not NULL THEN TRUE
    ELSE FALSE
END AS has_description,
CASE
    WHEN o.tag_list is not NULL THEN TRUE
    ELSE FALSE
END AS has_tags,
CASE
    WHEN o.tag_list is not NULL THEN LENGTH(tag_list)
    ELSE FALSE
END AS tag_count,
CASE
    WHEN o.license IS NOT NULL THEN license
    ELSE NULL
END AS license_code,
CASE
    WHEN o.license IS NOT NULL THEN TRUE
    ELSE FALSE
END AS has_license,
o.positional_accuracy AS positional_accuracy_m,
CASE WHEN o.coordinates_obscured is not Null THEN TRUE ELSE FALSE END AS obscured,
o.geoprivacy,
o.taxon_geoprivacy,
COALESCE(
    o.captive_cultivated IS NOT NULL,
    o.captive_cultivated
) AS captive,
o.oauth_application_id,
CASE WHEN o.user.orcid IS NOT NULL THEN TRUE ELSE FALSE END AS has_orcid,
o.owners_identification_from_vision AS owners_id_from_vision,


-- Community engagement (post-submission signals — use carefully, see notes)
--identifications_count,
--comments_count,
--faves_count,
--LENGTH(reviewed_by) AS reviewed_by_count,
--LENGTH(identifications) AS id_count_total,
--CASE WHEN outlinks is not NULL THEN TRUE ELSE FALSE END AS had_outlink,


-- Location (for geo features join)
o.latitude,
o.longitude,
o.place_guess,

-- Taxon (for taxon features join)
o.taxon_id,

-- Metadata
a.scraped_at,

FROM staged.observations o
LEFT JOIN raw.inat_api a ON o.uuid = a.raw_id
