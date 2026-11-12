import pytest

from helpers import CONFIG
from pipeline import bronze
from pipeline.config import check_month, load_settings


def _write(spark, month, values):
    df = spark.createDataFrame([(month, v) for v in values], "month STRING, v INT")
    bronze.write_month(df, "t_months", month)


def test_rerun_replaces_month_and_leaves_others_alone(spark):
    spark.sql("DROP TABLE IF EXISTS t_months")
    _write(spark, "2024-01", [1, 2])
    _write(spark, "2024-02", [10, 20, 30])
    _write(spark, "2024-02", [10, 20, 30])        # rerun: same rows, not doubled
    _write(spark, "2024-01", [7])                 # backfill January only
    got = {r["month"]: r["count"] for r in spark.table("t_months").groupBy("month").count().collect()}
    assert got == {"2024-01": 1, "2024-02": 3}


def test_as_strings_lowercases_names_and_keeps_values_as_text(spark):
    df = spark.createDataFrame([(1, 2.5)], "VendorID INT, Airport_fee DOUBLE")
    out = bronze.as_strings(df)
    assert out.dtypes == [("vendorid", "string"), ("airport_fee", "string")]
    assert out.first()["airport_fee"] == "2.5"


def test_load_trips_reads_the_month_folder(spark, tmp_path):
    spark.sql("CREATE DATABASE IF NOT EXISTS bt_bronze")
    spark.sql("DROP TABLE IF EXISTS bt_bronze.trips_raw")
    s = load_settings(CONFIG, catalog="spark_catalog", schema_prefix="bt")
    folder = tmp_path / "trips" / "month=2024-03"
    spark.createDataFrame([(1, "2024-03-01 10:00:00")], "VendorID INT, ts STRING") \
        .write.parquet(str(folder))
    bronze.load_trips(spark, s, "2024-03", str(tmp_path))
    row = spark.table("bt_bronze.trips_raw").first()
    assert row["vendorid"] == "1" and row["month"] == "2024-03"
    assert row["source_file"].endswith(".parquet") and row["ingested_at"] is not None


@pytest.mark.parametrize("bad", ["2024-13", "2024-1", "2024-01'; DROP TABLE x; --", ""])
def test_month_must_be_yyyy_mm(bad):
    with pytest.raises(ValueError):
        check_month(bad)
