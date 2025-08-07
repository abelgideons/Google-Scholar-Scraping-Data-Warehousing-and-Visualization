CREATE OR REPLACE TABLE silver_univ.dim_lecturer_interest AS
SELECT
  LOWER(REGEXP_REPLACE(REPLACE(TRIM(university), ' ', ''), r'(?i)(universitas|university)', '')) AS univ_id,
  TO_HEX(MD5(UPPER(REPLACE(CONCAT(name, university), ' ', '')))) AS lecturer_id,
  upper(name) as lecturer_name,
  TRIM(interest) AS research_interest
FROM `bronze_univ.*`,
UNNEST(SPLIT(research_interests, ',')) AS interest
WHERE TRIM(interest) IS NOT NULL AND TRIM(interest) <> ''
