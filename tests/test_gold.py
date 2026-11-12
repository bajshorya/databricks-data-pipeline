from datetime import datetime
from decimal import Decimal

import pytest

from helpers import CONFIG, SQL
from pipeline.config import load_settings
from pipeline.gold import build_gold

# Hand-made Silver trips: (pickup, borough, zone, earnings, minutes) x count.
# 2024-01-02 is a Tuesday, so every 09:00 trip below is in the slot (Weekday, 9) / Morning shift.
#  Midtown  Jan 2 + Apr 1: 200 x (20.00, 10)  -> 2.00 per minute, rank 1
#  JFK      Jan 2 + Feb 6: 100 x (60.00, 40) + 100 x (60.00, 30) -> 12000/7000, rank 2
#  Tiny     Jan 4:   4 x (20.00, 2) -> 10.00 per minute, but 4 pickups over 5 weekdays is under
#                    1 per hour, so left out
#  Outside of NYC Jan 2: 100 x (100.00, 10) -> best paying, but a catch-all zone, so left out
#  Jan 1 22:00: 1 x (10.00, 10) -> only there to give the moving average a first day
# Weekdays in the data: Jan 1, 2, 4, Feb 6, Apr 1 = 5.
# Daily earnings per minute: Jan 1 1.0, Jan 2 18000/6000 = 3.0, Jan 4 10.0 -> 7-day avg on Jan 4 = 14/3
# JFK per minute: Jan 1.5, Feb 2.0 -> +33.3%. City in April: March is missing -> change is NULL.
TRIPS = ([("2024-01-01 22:00", "Manhattan", "Midtown", "10.00", 10)]
         + [("2024-01-02 09:00", "Manhattan", "Midtown", "20.00", 10)] * 100
         + [("2024-01-02 09:00", "Queens", "JFK Airport", "60.00", 40)] * 100
         + [("2024-01-02 09:00", "N/A", "Outside of NYC", "100.00", 10)] * 100
         + [("2024-01-04 09:00", "Manhattan", "Tiny", "20.00", 2)] * 4
         + [("2024-02-06 09:00", "Queens", "JFK Airport", "60.00", 30)] * 100
         + [("2024-04-01 09:00", "Manhattan", "Midtown", "20.00", 10)] * 100)


@pytest.fixture(scope="module")
def gold(spark):
    for layer in ("silver", "gold"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS gd_{layer}")
    s = load_settings(CONFIG, catalog="spark_catalog", schema_prefix="gd")
    rows = [(ts[:7], datetime.strptime(ts, "%Y-%m-%d %H:%M"), b, z, Decimal(e), Decimal(m))
            for ts, b, z, e, m in TRIPS]
    spark.createDataFrame(rows, "month STRING, pickup_at TIMESTAMP, pickup_borough STRING, "
                                "pickup_zone STRING, driver_earnings DECIMAL(10,2), "
                                "trip_minutes DECIMAL(10,2)") \
        .write.format("delta").mode("overwrite").saveAsTable(s.table("silver", "trips"))
    build_gold(spark, s, SQL)
    return lambda name: spark.table(s.table("gold", name))


def rows_by(df, key):
    return {r[key]: r for r in df.collect()}


def test_daily_earnings_and_moving_average(gold):
    d = {str(r["trip_date"]): r for r in gold("daily_summary").collect()}
    assert d["2024-01-02"]["trips"] == 300 and d["2024-01-02"]["driver_earnings"] == Decimal("18000.00")
    assert float(d["2024-01-02"]["earnings_per_minute"]) == pytest.approx(3.0)
    assert float(d["2024-01-04"]["earnings_per_minute_7day_avg"]) == pytest.approx(14 / 3)


def test_zones_ranked_within_hour_and_quiet_or_catch_all_zones_left_out(gold):
    z = rows_by(gold("zone_hour_earnings").where("day_type = 'Weekday' AND pickup_hour = 9"), "zone")
    assert set(z) == {"Midtown", "JFK Airport"}
    assert float(z["Midtown"]["pickups_per_hour"]) == 40.0           # 200 pickups / 5 weekdays
    assert z["Midtown"]["zone_rank"] == 1 and float(z["Midtown"]["earnings_per_minute"]) == 2.0
    assert z["JFK Airport"]["zone_rank"] == 2
    assert float(z["JFK Airport"]["earnings_per_minute"]) == pytest.approx(12000 / 7000)


def test_shift_best_zones(gold):
    s = rows_by(gold("shift_best_zones").where("shift = '2 Morning (6-12)' AND day_type = 'Weekday'"),
                "zone")
    assert {z: r["zone_rank"] for z, r in s.items()} == {"Midtown": 1, "JFK Airport": 2}


def test_airport_vs_city_month_over_month(gold):
    a = gold("airport_vs_city")
    jfk = rows_by(a.where("pickup_type = 'JFK Airport'"), "month")
    assert jfk["2024-01"]["earnings_per_minute_change_pct"] is None   # no earlier month
    assert float(jfk["2024-02"]["earnings_per_minute_change_pct"]) == pytest.approx(1 / 3)
    assert float(jfk["2024-01"]["avg_trip_minutes"]) == 40.0
    city = rows_by(a.where("pickup_type = 'City'"), "month")
    assert city["2024-01"]["trips"] == 105                           # Outside of NYC not counted
    assert city["2024-04"]["earnings_per_minute_change_pct"] is None  # March is missing
