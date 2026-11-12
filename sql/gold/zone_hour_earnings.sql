-- "It's a weekday, 6 pm: where should I wait?" Each pickup zone's earnings per minute on a trip,
-- by weekday/weekend and hour, over all loaded months, ranked within each hour.
-- Earnings per minute is total earnings / total trip minutes, so one long trip can't dominate.
WITH days AS (
  -- How many weekdays and weekend days the data covers: each day gives each hour slot once.
  SELECT
    CASE WHEN dayofweek(pickup_at) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(DISTINCT CAST(pickup_at AS DATE)) AS days
  FROM {catalog}.{silver}.trips
  GROUP BY 1
),
by_zone AS (
  SELECT
    pickup_borough AS borough,
    pickup_zone AS zone,
    CASE WHEN dayofweek(pickup_at) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    hour(pickup_at) AS pickup_hour,
    COUNT(*) AS trips,
    SUM(driver_earnings) AS driver_earnings,
    SUM(trip_minutes) AS trip_minutes
  FROM {catalog}.{silver}.trips
  -- Catch-all zones (264 Unknown, 265 Outside of NYC) aren't places a driver can wait.
  WHERE pickup_borough NOT IN ('Unknown', 'N/A')
  GROUP BY 1, 2, 3, 4
)
SELECT
  borough, zone, day_type, pickup_hour, trips,
  trips / days AS pickups_per_hour,
  try_divide(driver_earnings, trips) AS avg_earnings_per_trip,
  try_divide(driver_earnings, trip_minutes) AS earnings_per_minute,
  RANK() OVER (PARTITION BY day_type, pickup_hour
               ORDER BY try_divide(driver_earnings, trip_minutes) DESC) AS zone_rank
FROM by_zone JOIN days USING (day_type)
-- Only zones where a driver can expect a fare: at least 1 pickup per hour on average.
-- Quiet zones with rare long trips would otherwise top the list (see docs/decisions.md).
WHERE trips / days >= 1
