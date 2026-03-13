SELECT COUNT(*) AS total_val
FROM features.observations,
WHERE created_at BETWEEN :val_start AND :test_start;
