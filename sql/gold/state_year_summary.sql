SELECT
  p.data_year,
  p.state,
  SUM(p.total_claims) AS total_claims,
  SUM(p.total_drug_cost) AS total_drug_cost,
  COUNT(DISTINCT p.npi) AS prescriber_count,
  try_divide(SUM(p.total_drug_cost), SUM(p.total_claims)) AS cost_per_claim,
  SUM(CASE WHEN d.is_opioid THEN p.total_claims ELSE 0 END) AS opioid_claims,
  try_divide(
    SUM(CASE WHEN d.is_opioid THEN p.total_claims ELSE 0 END),
    SUM(p.total_claims)
  ) AS opioid_claim_share
FROM {catalog}.{silver}.prescription p
LEFT JOIN {catalog}.{silver}.drug_year d
  ON p.data_year = d.data_year
 AND p.brand_name = d.brand_name
 AND p.generic_name = d.generic_name
GROUP BY p.data_year, p.state
