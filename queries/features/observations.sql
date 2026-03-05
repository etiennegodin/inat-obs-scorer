CREATE SCHEMA IF NOT EXISTS features;

CREATE OR REPLACE TABLE features.observations AS 


WITH labeled AS(

    SELECT o.id,

    COUNT(i.id) FILTER (
        WHERE o.created_at - i.created_at < INTERVAL '90 days'  
        AND o.species IS NOT NULL   
    ) ids_before,


    CASE WHEN ids_before > 0 THEN TRUE ELSE FALSE END AS label,


    FROM staged.observations o
    JOIN staged.identifications i ON o.id = i.observation_id
    GROUP BY o.id


)

SELECT 
-- Primary keys & joins
o.id AS observation_id,
o.uuid,
o.user.id AS user_id,

-- Label (the thing you're predicting)
l.label,
l.ids_before,
o.quality_grade as final_grade,

-- Temporal (submission-time signals)

o.observed_on,
o.created_at,
o.created_at - o.observed_on AS obs_to_submit_lag_days,
MONTH(o.observed_on) AS observed_month,
WEEK(o.observed_on) AS observed_week,
HOUR(observed_on_string) AS observed_hour,
HOUR(o.created_at) AS submitted_hour,
YEAR(o.created_at) AS submitted_year,

-- Documentation quality (submission-time)
LENGTH(o.observation_photos) AS photo_count,
CASE
    WHEN o.description is not NULL THEN TRUE
    ELSE FALSE
END as has_description,
CASE
    WHEN o.tag_list is not NULL THEN TRUE
    ELSE FALSE
END as has_tags,
CASE
    WHEN o.tag_list is not NULL THEN LENGTH(tag_list)
    ELSE FALSE
END as tag_count,
CASE 
    WHEN o.license IS NOT NULL THEN license
    ELSE NULL
END AS license_code,
CASE 
    WHEN o.license IS NOT NULL THEN TRUE
    ELSE FALSE
END AS has_license,
o.positional_accuracy as positional_accuracy_m,
CASE WHEN o.coordinates_obscured is not Null THEN TRUE ELSE FALSE END AS obscured,
o.geoprivacy,
o.taxon_geoprivacy,
COALESCE(
    o.captive_cultivated IS NOT NULL,
    o.captive_cultivated
) AS captive,
o.oauth_application_id,
CASE WHEN o.user.orcid IS NOT NULL THEN TRUE ELSE FALSE END AS has_orcid,
o.owners_identification_from_vision as owners_id_from_vision,


-- Community engagement (post-submission signals — use carefully, see notes)
--identifications_count,
--comments_count,
--faves_count,
--LENGTH(reviewed_by) as reviewed_by_count,
--LENGTH(identifications) as id_count_total,
--CASE WHEN outlinks is not NULL THEN TRUE ELSE FALSE END AS had_outlink,


-- Location (for geo features join)
o.latitude,
o.longitude,
o.place_guess,

-- Taxon (for taxon features join)
o.taxon_id

-- Metadata

FROM staged.observations o
JOIN labeled l ON o.id = l.id
