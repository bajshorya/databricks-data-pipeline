-- One row per day: what drivers earned and how much per minute on a trip,
-- with a 7-day moving average so weekday/weekend swings don't hide the trend.
WITH daily AS (
  SELECT
    month,
    CAST(pickup_at AS DATE) AS trip_date,
    COUNT(*) AS trips,
    SUM(driver_earnings) AS driver_earnings,
    SUM(trip_minutes) AS trip_minutes
  FROM {catalog}.{silver}.trips
  GROUP BY month, CAST(pickup_at AS DATE)
)
SELECT
  month, trip_date, trips, driver_earnings, trip_minutes,
  try_divide(driver_earnings, trip_minutes) AS earnings_per_minute,
  AVG(try_divide(driver_earnings, trip_minutes))
    OVER (ORDER BY trip_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS earnings_per_minute_7day_avg
FROM daily
