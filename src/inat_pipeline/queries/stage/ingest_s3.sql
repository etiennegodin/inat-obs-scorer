CREATE OR REPLACE TABLE {table_name} AS
SELECT {columns}
FROM read_csv_auto(:s3_path, header=true, sep='\t');
