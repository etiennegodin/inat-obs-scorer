CREATE OR REPLACE TABLE {table_name} AS
SELECT {columns}
FROM read_csv_auto(:source_dir, ignore_errors = :ignore)
