CREATE OR REPLACE TABLE gold_univ.gold_top_research_interest_per_univ AS
WITH ranked_interest AS (
  SELECT
    u.univ_id,
    u.university_name,
    i.research_interest,
    COUNT(DISTINCT i.lecturer_id) AS lecturer_count,
    ROW_NUMBER() OVER (PARTITION BY u.univ_id ORDER BY COUNT(DISTINCT i.lecturer_id) DESC) AS rank_in_univ
  FROM silver_univ.silver_lecturer_interest i
  JOIN silver_univ.dim_lecturer l ON i.lecturer_id = l.lecturer_id
  JOIN silver_univ.dim_university u ON l.univ_id = u.univ_id
  GROUP BY u.univ_id, u.university_name, i.research_interest
)
SELECT *
FROM ranked_interest
WHERE rank_in_univ <= 5;  -- ambil top 5 tiap universitas
