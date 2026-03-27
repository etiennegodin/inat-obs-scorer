-- Single source of truth for which observations are in scope for the no-ID model

CREATE OR REPLACE TABLE features.model_population AS

WITH score AS (

    SELECT *

    FROM research_grade_windowed(to_days(:score_window_days))

)

SELECT
    b.*,
    s.community_taxon_id,
    s.consensus_level_rg,
    t.rank_level AS community_taxon_rank
FROM features.base b
JOIN score s
    ON b.observation_id = s.observation_id
LEFT JOIN staged.taxa t ON s.community_taxon_id = t.taxon_id
WHERE
    s.is_rg = FALSE
    AND verifiable = TRUE  -- no-ID population only
