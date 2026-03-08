-- Not in original table
DELETE FROM raw.inat_api 
WHERE raw_id NOT IN (SELECT uuid FROM raw.obs_sample);
