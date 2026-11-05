WITH base AS (
  SELECT
    p.data_year,
    p.specialty,
    SUM(p.total_claims) AS total_claims,
    SUM(p.total_drug_cost) AS total_drug_cost,
    COUNT(DISTINCT p.npi) AS prescriber_count,
    SUM(CASE WHEN d.is_opioid THEN p.total_claims ELSE 0 END) AS opioid_claims
  FROM {catalog}.{silver}.prescription p
  LEFT JOIN {catalog}.{silver}.drug_year d
    ON p.data_year = d.data_year
   AND p.brand_name = d.brand_name
   AND p.generic_name = d.generic_name
  GROUP BY p.data_year, p.specialty
),
with_prior AS (
  SELECT
    *,
    LAG(data_year) OVER w AS prior_year,
    LAG(total_drug_cost) OVER w AS prior_cost
  FROM base
  WINDOW w AS (PARTITION BY specialty ORDER BY data_year)
)
SELECT
  data_year, specialty, total_claims, total_drug_cost, prescriber_count, opioid_claims,
  try_divide(total_drug_cost, prescriber_count) AS cost_per_prescriber,
  try_divide(opioid_claims, total_claims) AS opioid_claim_share,
  CASE WHEN prior_year = data_year - 1
       THEN try_divide(total_drug_cost - prior_cost, prior_cost) END AS yoy_cost_change_pct
FROM with_prior
