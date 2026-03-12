SELECT s.{source_key}
FROM {source_table_name} s
LEFT JOIN {target_table_name} t ON s.{source_key}  = t.raw_id
WHERE t.raw_id IS NULL
