CREATE OR REPLACE TABLE features.observers AS

SELECT user_id,
current_date as computed_as_of



FROM staged.users
