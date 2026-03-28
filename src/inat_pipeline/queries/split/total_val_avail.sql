SELECT COUNT(*) AS total_val
FROM features.model_population
WHERE created_at BETWEEN :val_start AND :test_start;
