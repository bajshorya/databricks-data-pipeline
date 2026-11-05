-- How does cost per claim vary across the project's states? Ranked bar.
SELECT data_year, state, cost_per_claim, total_claims, total_drug_cost
FROM workspace.partd_gold.state_year_summary
WHERE data_year = (SELECT MAX(data_year) FROM workspace.partd_gold.state_year_summary)
ORDER BY cost_per_claim DESC
