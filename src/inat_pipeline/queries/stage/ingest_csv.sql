CREATE OR REPLACE TABLE raw.{table_name} AS

SELECT {columns}
FROM read_csv_auto('{source}/*.csv',
ignore_errors=?)
