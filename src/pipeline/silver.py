from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.bronze import write_month
from pipeline.cleaning import is_bad_type, reject_reasons, safe_cast
from pipeline.config import Settings, check_month

# Silver column: (Bronze column, type). This is where data types get fixed.
TRIP_TYPES = {
    "vendor_id": ("vendorid", "int"),
    "pickup_at": ("tpep_pickup_datetime", "timestamp"),
    "dropoff_at": ("tpep_dropoff_datetime", "timestamp"),
    "passenger_count": ("passenger_count", "int"),
    "trip_distance": ("trip_distance", "double"),
    "pickup_location_id": ("pulocationid", "int"),
    "dropoff_location_id": ("dolocationid", "int"),
    "payment_type": ("payment_type", "int"),
    "fare_amount": ("fare_amount", "decimal(10,2)"),
    "tip_amount": ("tip_amount", "decimal(10,2)"),
    "total_amount": ("total_amount", "decimal(10,2)"),
}
# Two trips are the same trip if all of these match.
DUPLICATE_KEY = ["vendor_id", "pickup_at", "dropoff_at", "pickup_location_id",
                 "dropoff_location_id", "total_amount"]
BRONZE_EXTRA = ("source_file", "ingested_at", "month")
MAX_TRIP_MINUTES = 180  # see docs/decisions.md


def build_zones(zones_raw: DataFrame) -> DataFrame:
    return zones_raw.select(safe_cast("LocationID", "int").alias("location_id"),
                            F.col("Borough").alias("borough"), F.col("Zone").alias("zone"),
                            F.col("service_zone"))


def build_trips(bronze: DataFrame, zones: DataFrame, month: str, run_id: str):
    """Returns (trips, quarantine) for one month of Bronze rows."""
    source_cols = [c for c in bronze.columns if c not in BRONZE_EXTRA]
    df = bronze.withColumn("raw_row", F.to_json(F.struct(*source_cols)))
    # Before casting: some typed columns reuse the source name and would hide the bad text.
    df = df.withColumn("_bad_type", F.greatest(*[is_bad_type(src, typ)
                                                 for src, typ in TRIP_TYPES.values()]))
    for name, (src, typ) in TRIP_TYPES.items():
        df = df.withColumn(name, safe_cast(src, typ))

    for side in ("pickup", "dropoff"):
        z = F.broadcast(zones).select(
            F.col("location_id").alias(f"{side}_location_id"),
            F.col("borough").alias(f"{side}_borough"), F.col("zone").alias(f"{side}_zone"))
        df = df.join(z, f"{side}_location_id", "left")

    first = Window.partitionBy(*DUPLICATE_KEY).orderBy("raw_row")
    df = df.withColumn("_copy", F.row_number().over(first))

    rules = {
        "bad_type": F.col("_bad_type"),
        "missing_timestamp": F.col("pickup_at").isNull() | F.col("dropoff_at").isNull(),
        "dropoff_before_pickup": F.col("dropoff_at") <= F.col("pickup_at"),
        "outside_month": F.date_format("pickup_at", "yyyy-MM") != F.lit(month),
        "non_positive_distance": F.coalesce(F.col("trip_distance") <= 0, F.lit(True)),
        # Missing fare or tip would silently drop out of the earnings sums, so it's invalid too.
        "bad_amount": F.col("total_amount").isNull() | F.col("fare_amount").isNull()
                      | F.col("tip_amount").isNull() | (F.col("total_amount") < 0)
                      | (F.col("fare_amount") < 0) | (F.col("tip_amount") < 0),
        "unknown_zone": F.col("pickup_zone").isNull() | F.col("dropoff_zone").isNull(),
        "bad_passenger_count": ~F.col("passenger_count").between(0, 9),  # NULL is allowed
        # Meter left running (real data has a 6.5-day trip); would distort earnings per minute.
        "trip_too_long": (F.unix_timestamp("dropoff_at") - F.unix_timestamp("pickup_at"))
                         > MAX_TRIP_MINUTES * 60,
        "duplicate": F.col("_copy") > 1,
    }
    df = df.withColumn("reject_reasons", reject_reasons(rules))

    bad = df.where(F.size("reject_reasons") > 0)
    quarantine = bad.select("month", "reject_reasons", "raw_row", F.lit(run_id).alias("run_id"),
                            F.current_timestamp().alias("quarantined_at"))
    trips = df.where(F.size("reject_reasons") == 0).select(
        "month", *TRIP_TYPES, "pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone",
        ((F.unix_timestamp("dropoff_at") - F.unix_timestamp("pickup_at")) / 60)
        .cast("decimal(10,2)").alias("trip_minutes"),
        # What the driver keeps: taxes and surcharges go to the city, tolls only repay a cost.
        (F.col("fare_amount") + F.col("tip_amount")).cast("decimal(10,2)").alias("driver_earnings"))
    return trips, quarantine


def build_silver(spark: SparkSession, s: Settings, month: str, run_id: str) -> None:
    check_month(month)
    zones = build_zones(spark.table(s.table("bronze", "zones_raw")))
    zones.write.format("delta").mode("overwrite").saveAsTable(s.table("silver", "zones"))
    bronze = spark.table(s.table("bronze", "trips_raw")).where(F.col("month") == month)
    trips, quarantine = build_trips(bronze, spark.table(s.table("silver", "zones")), month, run_id)
    write_month(trips, s.table("silver", "trips"), month)
    write_month(quarantine, s.table("silver", "quarantine"), month)
