CREATE OR REPLACE TABLE raw.{table_name} AS

SELECT *
FROM read_csv_auto('{source}/*.csv',
ignore_errors=?)
