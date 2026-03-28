-- Single source of truth for which observations are in scope for the no-ID model

CREATE OR REPLACE TABLE features.model_population AS

SELECT b.*

FROM features.base b
JOIN features.identifications_at_window iw ON b.observation_id = iw.observation_id
WHERE
    iw.has_any_id = FALSE
