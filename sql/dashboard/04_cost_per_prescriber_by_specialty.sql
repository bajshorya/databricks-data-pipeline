-- Which specialties have the highest cost per prescriber? Ranked bar, at least 100 prescribers.
SELECT data_year, specialty, cost_per_prescriber, prescriber_count
FROM workspace.partd_gold.specialty_year_summary
WHERE data_year = (SELECT MAX(data_year) FROM workspace.partd_gold.specialty_year_summary)
  AND prescriber_count >= 100
ORDER BY cost_per_prescriber DESC
LIMIT 20
