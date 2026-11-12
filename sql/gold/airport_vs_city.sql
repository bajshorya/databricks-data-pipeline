-- Are airport pickups worth it? Airport vs city pickups per month, with change from the
-- previous month. The data has no waiting time, so the airport queue isn't counted.
WITH monthly AS (
  SELECT
    month,
    CASE WHEN pickup_zone IN ('JFK Airport', 'LaGuardia Airport', 'Newark Airport')
         THEN pickup_zone ELSE 'City' END AS pickup_type,
    COUNT(*) AS trips,
    SUM(driver_earnings) AS driver_earnings,
    SUM(trip_minutes) AS trip_minutes
  FROM {catalog}.{silver}.trips
  WHERE pickup_borough NOT IN ('Unknown', 'N/A')     -- catch-all zones aren't city pickups
  GROUP BY 1, 2
  -- Newark has ~50-80 yellow-cab pickups a month with odd values: too few to compare.
  HAVING COUNT(*) >= 100
),
with_prior AS (
  SELECT
    *,
    try_divide(driver_earnings, trip_minutes) AS earnings_per_minute,
    LAG(month) OVER w AS prior_month,
    LAG(try_divide(driver_earnings, trip_minutes)) OVER w AS prior_earnings_per_minute
  FROM monthly
  WINDOW w AS (PARTITION BY pickup_type ORDER BY month)
)
SELECT
  month, pickup_type, trips,
  try_divide(driver_earnings, trips) AS avg_earnings_per_trip,
  try_divide(trip_minutes, trips) AS avg_trip_minutes,
  earnings_per_minute,
  -- Only compare with the month directly before. A gap gives NULL, not a wrong change.
  CASE WHEN prior_month = date_format(add_months(to_date(month || '-01'), -1), 'yyyy-MM')
       THEN try_divide(earnings_per_minute - prior_earnings_per_minute, prior_earnings_per_minute)
  END AS earnings_per_minute_change_pct
FROM with_prior
