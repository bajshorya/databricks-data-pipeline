WITH base AS (
  SELECT
    data_year, brand_name, generic_name,
    SUM(total_claims) AS total_claims,
    SUM(total_drug_cost) AS total_drug_cost,
    COUNT(DISTINCT npi) AS prescriber_count
  FROM {catalog}.{silver}.prescription
  GROUP BY data_year, brand_name, generic_name
),
with_prior AS (
  SELECT
    *,
    LAG(data_year) OVER w AS prior_year,
    LAG(total_drug_cost) OVER w AS prior_cost
  FROM base
  WINDOW w AS (PARTITION BY brand_name, generic_name ORDER BY data_year)
)
SELECT
  data_year, brand_name, generic_name, total_claims, total_drug_cost, prescriber_count,
  try_divide(total_drug_cost, total_claims) AS cost_per_claim,
  CASE WHEN prior_year = data_year - 1 THEN total_drug_cost - prior_cost END AS yoy_cost_change,
  CASE WHEN prior_year = data_year - 1
       THEN try_divide(total_drug_cost - prior_cost, prior_cost) END AS yoy_cost_change_pct,
  RANK() OVER (PARTITION BY data_year ORDER BY total_drug_cost DESC) AS cost_rank
FROM with_prior
