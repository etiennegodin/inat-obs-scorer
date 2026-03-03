CREATE SCHEMA IF NOT EXISTS features;

CREATE OR REPLACE TABLE features.observations AS 

SELECT id AS observation_id,
uuid,
user.id AS user_id,
quality_grade,
CASE 
    WHEN quality_grade == 'research' THEN TRUE 
    WHEN captive_cultivated == TRUE THEN NULL
    ELSE FALSE
END AS label,
observed_on,
created_at,
created_at - observed_on AS obs_to_submit_lag_days,
MONTH(observed_on) AS observed_month,
WEEK(observed_on) AS observed_week,
HOUR(observed_on_string) AS observed_hour,
HOUR(created_at) AS submitted_hour,
YEAR(created_at) AS submitted_year,

LENGTH(observation_photos) AS photo_count,
CASE
    WHEN description is not NULL THEN TRUE
    ELSE FALSE
END as has_decscription,
CASE
    WHEN tag_list is not NULL THEN TRUE
    ELSE FALSE
END as has_tags,
CASE
    WHEN tag_list is not NULL THEN LENGTH(tag_list)
    ELSE FALSE
END as tag_count,
CASE 
    WHEN license IS NOT NULL THEN license
    ELSE NULL
END AS license_code,
CASE 
    WHEN license IS NOT NULL THEN TRUE
    ELSE FALSE
END AS has_license,
positional_accuracy as positional_accuracy_m,
CASE WHEN coordinates_obscured is not Null THEN TRUE ELSE FALSE END AS obscured,
geoprivacy,
taxon_geoprivacy,
CASE WHEN captive_cultivated IS NOT NULL THEN TRUE ELSE NULL END AS captive,
oauth_application_id,
CASE WHEN user.orcid IS NOT NULL THEN TRUE ELSE FALSE END AS has_orcid,
identifications_count,
comments_count,
faves_count,
LENGTH(reviewed_by) as reviewed_by_count,
owners_identification_from_vision as owners_id_from_vision,
LENGTH(identifications) as id_count_total,
CASE WHEN outlinks is not NULL THEN TRUE ELSE FALSE END AS had_outlink,
latitude,
longitude,
place_guess,
taxon_id

FROM staged.observations,
