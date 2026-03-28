-- Should return 0 rows ideally
SELECT
    genus,
    COUNT(*)
FROM staged.taxa_raw
WHERE "taxonRank" = 'genus'
GROUP BY genus
HAVING COUNT(*) > 1;
