SELECT
    submitted_year,
    COUNT(*) AS n
FROM features.model_population
GROUP BY 1
ORDER BY 1;
