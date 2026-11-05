-- Which drugs drove the largest yearly cost increases? Ranked bar, top 15.
SELECT data_year, brand_name, generic_name, yoy_cost_change, yoy_cost_change_pct, total_drug_cost
FROM workspace.partd_gold.drug_year_summary
WHERE data_year = (SELECT MAX(data_year) FROM workspace.partd_gold.drug_year_summary)
  AND yoy_cost_change IS NOT NULL
ORDER BY yoy_cost_change DESC
LIMIT 15
