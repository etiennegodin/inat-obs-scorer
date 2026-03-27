CREATE SCHEMA IF NOT EXISTS features;

CREATE OR REPLACE TABLE features.base AS

WITH first_taxon AS (

    SELECT
        o.id,
        i.taxon_id

    FROM staged.observations o
    JOIN staged.identifications i ON o.id = i.observation_id
    WHERE i.created_at = o.created_at

)

SELECT

-- Primary keys & joins
    o.id AS observation_id,
    o.uuid,
    o.user.id AS user_id,
    f.taxon_id AS init_taxon_id,
    t.rank_level AS init_rank_level,

    -- Temporal (submission-time signals)
    o.observed_on,
    o.created_at,
    o.created_at - o.observed_on AS obs_to_submit_lag_days,

    YEAR(o.created_at) AS submitted_year,

    SIN(2 * pi() * WEEK(o.observed_on) / 52) AS observed_week_sin,
    COS(2 * pi() * WEEK(o.observed_on) / 52) AS observed_week_cos,

    SIN(2 * pi() * MONTH(o.observed_on) / 12) AS observed_month_sin,
    COS(2 * pi() * MONTH(o.observed_on) / 12) AS observed_month_cos,

    SIN(2 * pi() * WEEK(o.created_at) / 52) AS submitted_week_sin,
    COS(2 * pi() * WEEK(o.created_at) / 52) AS submitted_week_cos,

    SIN(2 * pi() * MONTH(o.created_at) / 12) AS submitted_month_sin,
    COS(2 * pi() * MONTH(o.created_at) / 12) AS submitted_month_cos,

    --test

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
JOIN first_taxon f ON o.id = f.id
JOIN staged.taxa t ON t.taxon_id = f.taxon_id
WHERE
    o.created_at < :max_created_date
    AND o.captive_cultivated IS FALSE
