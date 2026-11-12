from decimal import Decimal

import pytest

from helpers import CONFIG
from pipeline import quality as q
from pipeline.config import load_settings


@pytest.fixture(scope="module")
def s(spark):
    for layer in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS qt_{layer}")
    return load_settings(CONFIG, catalog="spark_catalog", schema_prefix="qt")


def put(spark, s, layer, name, ddl, rows):
    t = s.table(layer, name)
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.createDataFrame(rows, ddl).write.format("delta").saveAsTable(t)


def counts(spark, s, bronze, good, bad):
    put(spark, s, "bronze", "trips_raw", "month STRING", [("2024-01",)] * bronze)
    put(spark, s, "silver", "trips", "month STRING", [("2024-01",)] * good)
    put(spark, s, "silver", "quarantine", "month STRING", [("2024-01",)] * bad)
    return {c.name: c for c in q.silver_checks(spark, s, "2024-01")}


def test_bronze_row_count(spark, s):
    put(spark, s, "bronze", "trips_raw", "month STRING", [("2024-01",)])
    assert q.bronze_checks(spark, s, "2024-01")[0].passed
    assert not q.bronze_checks(spark, s, "2024-02")[0].passed      # no rows for that month


def test_silver_checks_pass_on_clean_counts(spark, s):
    assert all(c.passed for c in counts(spark, s, 100, 98, 2).values())


def test_invalid_row_rate_over_limit_fails(spark, s):
    c = counts(spark, s, 100, 90, 10)                          # 10% invalid, limit 5%
    assert not c["invalid_row_rate"].passed and c["rows_reconcile"].passed


def test_rows_that_went_missing_fail_reconcile(spark, s):
    c = counts(spark, s, 100, 97, 1)                           # 2 rows unaccounted for
    assert not c["rows_reconcile"].passed and c["rows_reconcile"].observed == 2


def test_earnings_totals(spark, s):
    put(spark, s, "silver", "trips", "month STRING, driver_earnings DECIMAL(10,2)",
        [("2024-01", Decimal("10.50")), ("2024-01", Decimal("4.50"))])
    put(spark, s, "gold", "daily_summary", "month STRING, driver_earnings DECIMAL(20,2)",
        [("2024-01", Decimal("15.00"))])
    assert q.gold_checks(spark, s, "2024-01")[0].passed
    put(spark, s, "gold", "daily_summary", "month STRING, driver_earnings DECIMAL(20,2)",
        [("2024-01", Decimal("14.00"))])
    assert not q.gold_checks(spark, s, "2024-01")[0].passed


def test_all_results_logged_before_the_job_stops(spark, s):
    spark.sql(f"DROP TABLE IF EXISTS {s.table('ops', 'dq_results')}")
    checks = [q.Check("a", "silver", 1, 0, False), q.Check("b", "silver", 0, 0, True),
              q.Check("c", "silver", 1, 0, False)]
    with pytest.raises(q.QualityCheckError, match="'a', 'c'"):
        q.log_and_stop(spark, s, checks, "run9", "2024-01")
    got = {r["check_name"]: r["status"] for r in spark.table(s.table("ops", "dq_results")).collect()}
    assert got == {"a": "fail", "b": "pass", "c": "fail"}
