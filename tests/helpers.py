"""Small hand-made rows in the same shape as Bronze: every value a string, lowercase names."""
from pathlib import Path

from pyspark.sql import functions as F

ROOT = Path(__file__).parent.parent
CONFIG = str(ROOT / "config")
SQL = str(ROOT / "sql")

COLS = ["vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
        "trip_distance", "pulocationid", "dolocationid", "payment_type", "fare_amount",
        "tip_amount", "total_amount"]


def trip(pickup="2024-01-10 08:00:00", dropoff="2024-01-10 08:20:00", distance="2.5",
         fare="15.00", total="20.00", pu="1", do="2", passengers="1", vendor="2", payment="1",
         tip="2.00"):
    return {"vendorid": vendor, "tpep_pickup_datetime": pickup, "tpep_dropoff_datetime": dropoff,
            "passenger_count": passengers, "trip_distance": distance, "pulocationid": pu,
            "dolocationid": do, "payment_type": payment, "fare_amount": fare, "tip_amount": tip,
            "total_amount": total}


def bronze_frame(spark, rows, month="2024-01"):
    df = spark.createDataFrame([tuple(r[c] for c in COLS) for r in rows],
                               ", ".join(f"{c} STRING" for c in COLS))
    return (df.withColumn("source_file", F.lit("test.parquet"))
              .withColumn("ingested_at", F.current_timestamp()).withColumn("month", F.lit(month)))


def zones_raw(spark):
    return spark.createDataFrame(
        [("1", "EWR", "Newark Airport", "EWR"), ("2", "Queens", "Jamaica Bay", "Boro Zone"),
         ("3", "Manhattan", "Midtown", "Yellow Zone")],
        "LocationID STRING, Borough STRING, Zone STRING, service_zone STRING")
