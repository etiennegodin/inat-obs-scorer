CREATE OR REPLACE TABLE raw.obs_sample AS

WITH user_obs AS (
    SELECT
        user_id,
        COUNT(DISTINCT(uuid)) AS obs_count,
        MAX(observed_on) AS newest,
        MIN(observed_on) AS oldest

    FROM raw.downloads
    GROUP BY user_id
)

SELECT
    u.obs_count,
    d.*

FROM raw.downloads d
JOIN user_obs u ON u.user_id = d.user_id

WHERE
    observed_on >= DATE('2010-01-01')
    AND observed_on IS NOT NULL
    AND u.obs_count >= 20
    AND u.oldest <= DATE('2020-01-01')
    AND u.newest >= DATE('2024-01-01')
