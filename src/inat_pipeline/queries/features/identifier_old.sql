CREATE OR REPLACE TABLE features.identifiers_score AS

SELECT
-- Keys
i.observation_id,
i.user_id,
u.identifier_only,

-- Volume
COUNT(*) OVER identifier_history AS total_id_count,
COUNT(*) FILTER (WHERE i.own_observation IS FALSE) OVER identifier_history AS ids_given_count,
CASE WHEN ids_given_count > 0 THEN TRUE ELSE FALSE END AS gave_id,

-- Community
COUNT(DISTINCT(i.id)) FILTER (
    WHERE i.category = 'supporting'
    ) OVER identifier_history AS ids_given_agreeing,
ids_given_agreeing / ids_given_count AS ids_given_agreeing_pct,

-- Ai vision
COUNT(DISTINCT(i.id)) FILTER (
    WHERE i.vision = TRUE
    ) OVER identifier_history AS ids_given_vision,
ids_given_vision / ids_given_count AS ids_given_vision_pct,

-- Taxonomic behaviour
COUNT(DISTINCT(i.order)) FILTER (WHERE i.order IS NOT NULL) OVER taxon_history AS taxon_diversity_order,
COUNT(DISTINCT(i.family)) FILTER (WHERE i.family IS NOT NULL) OVER taxon_history AS taxon_diversity_family,
COUNT(DISTINCT(i.genus)) FILTER (WHERE i.genus IS NOT NULL) OVER taxon_history AS taxon_diversity_genus,
COUNT(DISTINCT(i.species)) FILTER (WHERE i.species IS NOT NULL) OVER taxon_history AS taxon_diversity_species,

FROM staged.identifications i
JOIN staged.users u ON u.user_id = i.user_id

WINDOW
    identifier_history AS (
        PARTITION BY i.user_id
        ORDER BY i.created_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ),
    taxon_history AS (
    PARTITION BY i.taxon_id
    ORDER BY i.created_at
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )
