CREATE SCHEMA IF NOT EXISTS tests;

CREATE OR REPLACE TABLE tests.community_taxon AS
SELECT * 
FROM community_taxon_windowed(INTERVAL '999 years');


CREATE OR REPLACE TABLE tests.community_taxon_comparison AS

SELECT 
    -- raw data
    c.observation_id,
    c.community_taxon,
    o.taxon_id,
    c.rank,
    o."taxonRank",

    -- comparisons
    c.rank = o."taxonRank" AS correct_taxon_level,
    c.community_taxon = o.taxon_id AS correct_taxon,

FROM community_taxon_windowed(INTERVAL '999 years') c
LEFT JOIN staged.observations o ON o.id = c.observation_id;

CREATE OR REPLACE TABLE tests.community_taxon_metrics AS
SELECT
    SUM(correct_taxon_level) / COUNT(observation_id) AS tx_lvl_accuracy ,
    SUM(correct_taxon) / COUNT(observation_id) AS tx_id_accuracy,

FROM tests.community_taxon_comparison











