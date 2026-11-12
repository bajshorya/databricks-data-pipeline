from decimal import Decimal

from helpers import bronze_frame, trip, zones_raw
from pipeline import silver


def build(spark, rows):
    zones = silver.build_zones(zones_raw(spark))
    return silver.build_trips(bronze_frame(spark, rows), zones, "2024-01", "run1")


def reasons(quarantine):
    return [sorted(r["reject_reasons"]) for r in quarantine.collect()]


def test_valid_row_is_typed_and_joined_to_zones(spark):
    trips, q = build(spark, [trip()])
    assert q.count() == 0
    row = trips.first()
    assert row["total_amount"] == Decimal("20.00") and row["pickup_location_id"] == 1
    assert row["pickup_borough"] == "EWR" and row["dropoff_zone"] == "Jamaica Bay"
    assert row["trip_minutes"] == Decimal("20.00")
    assert row["driver_earnings"] == Decimal("17.00")      # fare 15 + tip 2
    assert dict(trips.dtypes)["pickup_at"] == "timestamp"


def test_each_rule_has_its_reason(spark):
    cases = {
        "bad_type": trip(passengers="two"),
        "missing_timestamp": trip(pickup=None),
        "dropoff_before_pickup": trip(dropoff="2024-01-10 07:00:00"),
        "outside_month": trip(pickup="2023-12-31 23:50:00", dropoff="2024-01-01 00:10:00"),
        "non_positive_distance": trip(distance="0"),
        "bad_amount": trip(fare="-5.00"),
        "trip_too_long": trip(dropoff="2024-01-10 11:01:00"),   # 181 minutes
        "unknown_zone": trip(pu="999"),
        "bad_passenger_count": trip(passengers="12"),
    }
    for code, row in cases.items():
        _, q = build(spark, [row])
        assert code in reasons(q)[0], (code, reasons(q))


def test_row_can_have_several_reasons(spark):
    _, q = build(spark, [trip(distance="0", total="-1.00")])
    assert reasons(q) == [["bad_amount", "non_positive_distance"]]


def test_missing_or_negative_tip_is_a_bad_amount(spark):
    _, q = build(spark, [trip(tip=None), trip(tip="-1.00", pu="3")])
    assert reasons(q) == [["bad_amount"], ["bad_amount"]]


def test_three_hour_trip_is_allowed(spark):
    trips, q = build(spark, [trip(dropoff="2024-01-10 11:00:00")])
    assert q.count() == 0 and trips.count() == 1


def test_null_passenger_count_is_allowed(spark):
    trips, q = build(spark, [trip(passengers=None)])
    assert q.count() == 0 and trips.first()["passenger_count"] is None


def test_duplicates_keep_one_copy_and_quarantine_the_rest(spark):
    trips, q = build(spark, [trip(), trip(), trip(), trip(pu="3")])
    assert trips.count() == 2
    assert reasons(q) == [["duplicate"], ["duplicate"]]


def test_every_input_row_ends_up_in_exactly_one_table(spark):
    rows = [trip(), trip(), trip(distance="0"), trip(pu="3"), trip(pickup=None)]
    trips, q = build(spark, rows)
    assert trips.count() + q.count() == len(rows)
    assert all('"trip_distance"' in r["raw_row"] for r in q.collect())   # original row is kept
