CREATE OR REPLACE TABLE {table_name} AS
SELECT {columns}
FROM {source_function}(:s3_path{source_options});
