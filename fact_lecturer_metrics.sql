CREATE OR REPLACE TABLE silver_univ.fact_lecturer_metrics AS
with base as 
(
  SELECT
    -- Foreign keys ke dimensi

    LOWER(REGEXP_REPLACE(REPLACE(TRIM(university), ' ', ''), r'(?i)(universitas|university)', '')) AS univ_id,
    TO_HEX(MD5(UPPER(REPLACE(CONCAT(name, university), ' ', '')))) AS lecturer_id,
    upper(university) as university_name,
    upper(name) as lecturer_name,
    profile_url,
    -- Metrics
    SAFE_CAST(cited_by AS INT64) AS cited_by,
    SAFE_CAST(citations_all AS INT64) AS citations_all,
    SAFE_CAST(citations_since_2020 AS INT64) AS citations_since_2020,
    SAFE_CAST(h_index_all AS INT64) AS h_index_all,
    SAFE_CAST(h_index_since_2020 AS INT64) AS h_index_since_2020,
    SAFE_CAST(i10_index_all AS INT64) AS i10_index_all,
    SAFE_CAST(i10_index_since_2020 AS INT64) AS i10_index_since_2020
  FROM `bronze_univ.*`
),
research as 
(
  select
    univ_id,
    lecturer_id,
    count(distinct research_interest) as count_research_interest
  from silver_univ.silver_lecturer_interest
  group by all
)
select 
  current_date() as process_date,
  b.univ_id,
  b.lecturer_id,
  b.university_name,
  b.lecturer_name,
  b.profile_url,
  b.cited_by,
  b.citations_all,
  b.citations_since_2020,
  b.h_index_all,
  b.h_index_since_2020,
  b.i10_index_all,
  b.i10_index_since_2020,
  r.count_research_interest as interest_topics
from base as b 
left join research as r on b.univ_id = r.univ_id and b.lecturer_id = r.lecturer_id







