-- For each observation, what is the average phenological activity
-- of its confusion neighbors at the same time?
-- High = many similar species are also active = harder to identify

CREATE OR REPLACE TABLE features.taxon_histo AS

SELECT
    taxon_id,
    blended_pmf AS o_blended_pmf

FROM blended_histogram(week_map_observed, 100);


CREATE OR REPLACE TABLE features.taxon_histo_conf AS

SELECT
    o.id AS observation_id,
    -- avg activity of confusion neighbors at observation week
    AVG(
        neighbor_pheno.o_blended_pmf[LEAST(WEEK(o.observed_on), 52)]
    ) AS confusion_nbrhd_pheno_activity,

    -- ratio: how active is focal taxon vs its neighbors?
    -- < 1 means neighbors more active than focal = harder conditions
    ot.o_blended_pmf[LEAST(WEEK(o.observed_on), 52)]
    / NULLIF(AVG(neighbor_pheno.o_blended_pmf[LEAST(WEEK(o.observed_on), 52)]), 0)
        AS focal_vs_nbrhd_pheno_ratio

FROM staged.observations o
JOIN features.taxon_histo ot ON o.taxon_id = ot.taxon_id
JOIN staged.similar_species e ON o.taxon_id = e.taxon_id
JOIN features.taxon_histo neighbor_pheno ON e.similar_taxon_id = neighbor_pheno.taxon_id
GROUP BY o.id, ot.o_blended_pmf, WEEK(o.observed_on)
