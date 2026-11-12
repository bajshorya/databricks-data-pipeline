"""One function per job task. jobs/run_task.py calls these on Databricks; tests call them locally."""
import argparse
import os

from pyspark.sql import SparkSession

from pipeline import bronze, gold, quality, silver
from pipeline.config import Settings, check_month, load_settings


def make_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--task", required=True, choices=sorted(TASKS))
    ap.add_argument("--month", required=True,
                    type=lambda m: m if m == "latest" else check_month(m))
    ap.add_argument("--run-id", default="local")
    ap.add_argument("--catalog", default=None)
    ap.add_argument("--schema-prefix", default="pipeline")
    ap.add_argument("--src-path")
    ap.add_argument("--config-dir", required=True)
    ap.add_argument("--sql-dir", required=True)
    ap.add_argument("--landing-root", default=None, help="override, used by local tests")
    return ap


def settings(a) -> Settings:
    return load_settings(a.config_dir, a.catalog, a.schema_prefix)


def load_bronze(spark: SparkSession, a) -> None:
    s = settings(a)
    for schema in (s.bronze, s.silver, s.gold, s.ops):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s.catalog}.{schema}")
    root = a.landing_root or s.landing_root
    bronze.load_zones(spark, s, root)
    bronze.load_trips(spark, s, a.month, root)


def check_bronze(spark: SparkSession, a) -> None:
    s = settings(a)
    quality.log_and_stop(spark, s, quality.bronze_checks(spark, s, a.month), a.run_id, a.month)


def build_silver(spark: SparkSession, a) -> None:
    silver.build_silver(spark, settings(a), a.month, a.run_id)


def check_silver(spark: SparkSession, a) -> None:
    s = settings(a)
    quality.log_and_stop(spark, s, quality.silver_checks(spark, s, a.month), a.run_id, a.month)


def build_gold(spark: SparkSession, a) -> None:
    gold.build_gold(spark, settings(a), a.sql_dir)


def check_gold(spark: SparkSession, a) -> None:
    s = settings(a)
    quality.log_and_stop(spark, s, quality.gold_checks(spark, s, a.month), a.run_id, a.month)


TASKS = {f.__name__: f for f in (load_bronze, check_bronze, build_silver, check_silver,
                                 build_gold, check_gold)}


def latest_month(landing_root: str) -> str:
    """Newest month folder that has landed, e.g. trips/month=2024-03 -> '2024-03'."""
    months = [d.removeprefix("month=") for d in os.listdir(f"{landing_root}/trips")
              if d.startswith("month=")]
    if not months:
        raise ValueError(f"no month folders under {landing_root}/trips")
    return check_month(max(months))


def run(spark: SparkSession, a) -> None:
    """The monthly schedule passes month=latest: process the newest landed month.
    shortcut: each task resolves 'latest' on its own, so don't upload a new month mid-run."""
    if a.month == "latest":
        a.month = latest_month(a.landing_root or settings(a).landing_root)
    TASKS[a.task](spark, a)
