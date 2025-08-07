
create or replace table `silver_univ.dim_university` as
select 
  distinct upper(university) as university_name,
  LOWER(REGEXP_REPLACE(REPLACE(TRIM(university), ' ', ''), r'(?i)(university|universitas)', '')) AS univ_id 
FROM `bronze_univ.*`


