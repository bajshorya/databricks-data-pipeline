-- How has opioid claim share changed by specialty? Grouped bar, 2023 vs 2024, top 8 specialties by claims.
WITH top AS (
  SELECT specialty FROM workspace.partd_gold.specialty_year_summary
  GROUP BY specialty ORDER BY SUM(total_claims) DESC LIMIT 8
)
SELECT s.data_year, s.specialty, s.opioid_claim_share
FROM workspace.partd_gold.specialty_year_summary s
JOIN top USING (specialty)
ORDER BY s.specialty, s.data_year
