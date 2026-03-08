-- Check how common the ambiguous ones are in your identifications
SELECT t."taxonRank", COUNT(*) AS n
FROM staged.identifications i
JOIN staged.taxa_raw t ON i.taxon_id = t.id
WHERE t."taxonRank" IN ('complex', 'genushybrid', 'hybrid', 'infrahybrid')
GROUP BY t."taxonRank"
ORDER BY n DESC;