

CREATE OR REPLACE TABLE gold_univ.gold_top_lecturer_by_university AS
SELECT
  f.univ_id,
  u.university_name,
  f.lecturer_id,
  l.lecturer_name,
  f.citations_all,
  f.citations_since_2020,
  (f.citations_all - f.citations_since_2020)/f.citations_since_2020 as growth_rate,
  ROW_NUMBER() OVER (PARTITION BY f.univ_id ORDER BY f.citations_all DESC) AS rank_in_univ,
  ROW_NUMBER() OVER (PARTITION BY f.univ_id ORDER BY f.citations_since_2020 DESC) AS rank_in_univ_2020,
FROM silver_univ.fact_lecturer_metrics f
JOIN silver_univ.dim_university u ON f.univ_id = u.univ_id
JOIN silver_univ.dim_lecturer l ON f.lecturer_id = l.lecturer_id;
