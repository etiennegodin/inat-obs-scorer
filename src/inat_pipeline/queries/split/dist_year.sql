SELECT submitted_year, COUNT(*) AS n
FROM features.observations
GROUP BY 1
ORDER BY 1;
