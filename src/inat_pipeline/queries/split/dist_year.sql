SELECT submitted_year, COUNT(*) AS n
FROM features.base
GROUP BY 1
ORDER BY 1;
