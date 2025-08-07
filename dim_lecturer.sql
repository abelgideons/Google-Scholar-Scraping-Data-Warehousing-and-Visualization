
create or replace table `silver_univ.dim_lecturer` as
SELECT
  LOWER(REGEXP_REPLACE(REPLACE(TRIM(university), ' ', ''), r'(?i)(university|universitas)', '')) AS univ_id,
  TO_HEX(MD5(UPPER(REPLACE(CONCAT(name, university), ' ', '')))) AS lecturer_id,
  UPPER(TRIM(name)) AS lecturer_name,
  profile_url,
  research_interests
FROM `bronze_univ.*`

