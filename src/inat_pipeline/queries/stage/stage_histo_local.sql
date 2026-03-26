CREATE OR REPLACE TABLE staged.histogram_created_local AS
SELECT * FROM histogram_local(created_at);

CREATE OR REPLACE TABLE staged.histogram_observed_local AS
SELECT * FROM histogram_local(observed_on);
