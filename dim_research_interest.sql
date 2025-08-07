create or replace table `silver_univ.dim_research_interest` as
WITH campus AS 
(
  SELECT
    university,
    md5(upper(replace(concat(name, university), ' ', ''))) AS lecturer_id,
    SPLIT(research_interests, ',') AS interests
  FROM `bronze_univ.*`
)
SELECT
  university,
  lecturer_id,
  TRIM(interest) AS research_interest
FROM campus, UNNEST(interests) AS interest
WHERE TRIM(interest) IS NOT NULL AND TRIM(interest) != ''
GROUP BY all

