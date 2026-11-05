WITH detail AS (
  SELECT
    data_year, state,
    SUM(total_claims) AS detail_claims,
    SUM(total_drug_cost) AS detail_cost
  FROM {catalog}.{silver}.prescription
  GROUP BY data_year, state
),
geo AS (
  SELECT
    data_year, geo_code AS state,
    SUM(total_claims) AS geo_claims,
    SUM(total_drug_cost) AS geo_cost
  FROM {catalog}.{silver}.geo_drug
  WHERE geo_level = 'State'
  GROUP BY data_year, geo_code
)
SELECT
  d.data_year, d.state, d.detail_claims, g.geo_claims, d.detail_cost, g.geo_cost,
  try_divide(d.detail_claims, g.geo_claims) AS claim_coverage,
  try_divide(d.detail_cost, g.geo_cost) AS cost_coverage
FROM detail d
LEFT JOIN geo g ON d.data_year = g.data_year AND d.state = g.state
