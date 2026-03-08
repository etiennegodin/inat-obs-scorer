SELECT s.uuid
FROM raw.obs_sample s
LEFT JOIN raw.inat_api t ON s.uuid  = t.item_key
WHERE t.item_key IS NULL