-- Top 5 pickup zones for each shift (weekday and weekend), by earnings per minute on a trip,
-- among zones with at least 1 pickup per hour on average.
WITH with_slots AS (
  SELECT
    *,
    CASE WHEN hour(pickup_at) < 6 THEN '1 Night (0-6)'
         WHEN hour(pickup_at) < 12 THEN '2 Morning (6-12)'
         WHEN hour(pickup_at) < 18 THEN '3 Afternoon (12-18)'
         ELSE '4 Evening (18-24)' END AS shift,
    CASE WHEN dayofweek(pickup_at) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type
  FROM {catalog}.{silver}.trips
),
days AS (
  SELECT day_type, COUNT(DISTINCT CAST(pickup_at AS DATE)) AS days FROM with_slots GROUP BY 1
),
by_shift AS (
  SELECT
    shift, day_type,
    pickup_borough AS borough,
    pickup_zone AS zone,
    COUNT(*) AS trips,
    try_divide(SUM(driver_earnings), SUM(trip_minutes)) AS earnings_per_minute,
    try_divide(SUM(driver_earnings), COUNT(*)) AS avg_earnings_per_trip
  FROM with_slots
  WHERE pickup_borough NOT IN ('Unknown', 'N/A')     -- catch-all zones, see zone_hour_earnings
  GROUP BY 1, 2, 3, 4
),
ranked AS (
  SELECT
    by_shift.*,
    trips / (days * 6) AS pickups_per_hour,            -- each shift is 6 hours
    RANK() OVER (PARTITION BY shift, by_shift.day_type ORDER BY earnings_per_minute DESC) AS zone_rank
  FROM by_shift JOIN days USING (day_type)
  WHERE trips / (days * 6) >= 1
)
SELECT * FROM ranked WHERE zone_rank <= 5
