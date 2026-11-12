from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.config import Settings, check_month


class QualityCheckError(Exception):
    pass


@dataclass(frozen=True)
class Check:
    name: str
    layer: str
    observed: float
    threshold: float
    passed: bool


def _n(spark: SparkSession, query: str) -> float:
    return float(spark.sql(query).first()[0] or 0)


def log_and_stop(spark: SparkSession, s: Settings, checks: list[Check], run_id: str,
                 month: str) -> None:
    """Log every result first, then raise if any failed, so one failure never hides the others.
    The raise fails the Databricks task, and the job skips every task after it."""
    rows = [(run_id, c.name, c.layer, month, float(c.observed), float(c.threshold),
             "pass" if c.passed else "fail") for c in checks]
    (spark.createDataFrame(rows, "run_id STRING, check_name STRING, layer STRING, month STRING, "
                                 "observed_value DOUBLE, threshold DOUBLE, status STRING")
     .withColumn("checked_at", F.current_timestamp())
     .write.format("delta").mode("append").saveAsTable(s.table("ops", "dq_results")))
    failed = [c.name for c in checks if not c.passed]
    if failed:
        raise QualityCheckError(f"checks failed for {month}: {failed}")


def bronze_checks(spark: SparkSession, s: Settings, month: str) -> list[Check]:
    m = check_month(month)
    n = _n(spark, f"SELECT COUNT(*) FROM {s.table('bronze', 'trips_raw')} WHERE month = '{m}'")
    return [Check("bronze_row_count", "bronze", n, 0, n > 0)]


def silver_checks(spark: SparkSession, s: Settings, month: str) -> list[Check]:
    m = check_month(month)
    bronze = _n(spark, f"SELECT COUNT(*) FROM {s.table('bronze', 'trips_raw')} WHERE month = '{m}'")
    good = _n(spark, f"SELECT COUNT(*) FROM {s.table('silver', 'trips')} WHERE month = '{m}'")
    bad = _n(spark, f"SELECT COUNT(*) FROM {s.table('silver', 'quarantine')} WHERE month = '{m}'")
    pct = 100 * bad / bronze if bronze else 0
    return [Check("invalid_row_rate", "silver", pct, s.invalid_row_max_pct,
                  pct < s.invalid_row_max_pct),
            Check("rows_reconcile", "silver", bronze - good - bad, 0, bronze == good + bad)]


def gold_checks(spark: SparkSession, s: Settings, month: str) -> list[Check]:
    m = check_month(month)
    silver = _n(spark, f"SELECT SUM(driver_earnings) FROM {s.table('silver', 'trips')} WHERE month = '{m}'")
    gold = _n(spark, f"SELECT SUM(driver_earnings) FROM {s.table('gold', 'daily_summary')} WHERE month = '{m}'")
    return [Check("earnings_totals_match", "gold", gold - silver, 0, gold == silver)]
