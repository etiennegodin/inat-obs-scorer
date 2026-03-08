CREATE SCHEMA IF NOT EXISTS tests;

CREATE OR REPLACE TABLE tests.community_taxon AS

SELECT 

    -- raw data
    c.observation_id,
    c.community_taxon,
    o.taxon_id,
    c.consensus_level,
    o."taxonRank",

    -- comparisons
    c.consensus_level = o."taxonRank" AS correct_taxon_level
    c.community_taxon = o.taxon_id AS correct_taxon,



FROM community_taxon_windowed(INTERVAL '999 years') c
LEFT JOIN staged.observations o ON o.id = c.observation_id;





