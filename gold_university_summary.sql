CREATE OR REPLACE TABLE gold_univ.gold_university_summary AS
SELECT
  f.univ_id,
  u.university_name,
  COUNT(DISTINCT f.lecturer_id) AS total_lecturer,
  SUM(f.citations_all) AS total_citations,
  AVG(f.citations_all) AS avg_citations_per_lecturer,
  AVG(f.h_index_all) AS avg_h_index,
  AVG(f.i10_index_all) AS avg_i10_index
FROM silver_univ.fact_lecturer_metrics f
JOIN silver_univ.dim_university u ON f.univ_id = u.univ_id
GROUP BY 1,2;
