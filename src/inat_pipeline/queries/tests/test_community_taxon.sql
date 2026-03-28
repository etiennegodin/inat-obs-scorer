CREATE SCHEMA IF NOT EXISTS tests;

-- Get data from community taxon at current time

CREATE OR REPLACE TABLE tests.community_taxon AS
SELECT *
FROM community_taxon_windowed(INTERVAL '999 years');


-- Load scraped data and compare with computed values
CREATE OR REPLACE TABLE tests.community_taxon_comparison AS

SELECT
    -- raw data
    c.observation_id,
    c.community_taxon AS c_taxon_id,
    o.taxon_id AS o_taxon_id,
    c.rank_level AS c_rank,
    o.rank_level AS o_rank,
    o.quality_grade = 'research' AS o_rg,
    rg.is_rg AS c_rg,
    -- comparisons
    c.rank_level <= o.rank_level AS correct_taxon_level,
    c.community_taxon = o.taxon_id AS correct_taxon

FROM tests.community_taxon c
LEFT JOIN staged.observations o ON o.id = c.observation_id
LEFT JOIN research_grade_windowed(INTERVAL '999 years') rg ON c.observation_id = rg.observation_id;


CREATE OR REPLACE TABLE tests.community_taxon_metrics AS
SELECT
    SUM(correct_taxon_level) / COUNT(observation_id) FILTER (
        WHERE o_rg IS TRUE
        AND c_rg IS TRUE
    ) AS rg_tx_lvl_accuracy,

    SUM(correct_taxon) / COUNT(observation_id) FILTER (
        WHERE o_rg IS TRUE
        AND c_rg IS TRUE
    ) AS rg_tx_id_accuracy,

    SUM(correct_taxon_level) / COUNT(observation_id) AS all_tx_lvl_accuracy,
    SUM(correct_taxon) / COUNT(observation_id) AS all_tx_id_accuracy,

FROM tests.community_taxon_comparison;

CREATE OR REPLACE TABLE tests.community_taxon_wrong_rg AS
SELECT *

FROM tests.community_taxon_comparison
WHERE
    o_rg
    AND NOT correct_taxon_level
;

CREATE OR REPLACE TABLE tests.community_taxon_wrong_rg_2 AS
SELECT *

FROM tests.community_taxon_comparison
WHERE
    NOT o_rg
    AND NOT correct_taxon_level
;
