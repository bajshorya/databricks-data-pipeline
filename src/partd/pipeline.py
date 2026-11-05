import argparse
import glob

from pyspark.sql import SparkSession

from partd import bronze, gold, quality, silver
from partd.config import Settings, load_settings

DATASETS = ("provider_drug", "geo_drug")


def make_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-year", type=int, required=True)
    ap.add_argument("--force", default="false")
    ap.add_argument("--run-id", default="local")
    ap.add_argument("--catalog", default=None)
    ap.add_argument("--schema-prefix", default="partd")
    ap.add_argument("--src-path")
    ap.add_argument("--config-dir", required=True)
    ap.add_argument("--sql-dir", required=True)
    ap.add_argument("--landing-root", default=None, help="override, used by local tests")
    return ap


def settings(a) -> Settings:
    return load_settings(a.config_dir, a.catalog, a.schema_prefix)


def ensure_schemas(spark: SparkSession, s: Settings) -> None:
    for schema in (s.bronze, s.silver, s.gold, s.ops):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s.catalog}.{schema}")


def run_bronze(spark: SparkSession, a) -> dict:
    s = settings(a)
    ensure_schemas(spark, s)
    root = a.landing_root or s.landing_root
    results = {}
    for ds in DATASETS:
        files = glob.glob(f"{root}/{ds}/data_year={a.data_year}/*.csv.gz")
        if len(files) != 1:
            raise bronze.BronzeError(f"expected one file for {ds} {a.data_year}, found {files}")
        results[ds] = bronze.load_bronze(spark, files[0], ds, a.data_year, a.config_dir,
                                         f"{s.catalog}.{s.bronze}", a.force.lower() == "true")
        quality.log_and_gate(spark, s, quality.bronze_checks(spark, s, ds, a.data_year),
                             a.run_id, a.data_year)
    return results


def run_silver(spark: SparkSession, a) -> None:
    silver.build_silver(spark, settings(a), a.data_year, a.run_id)


def run_check_silver(spark: SparkSession, a) -> None:
    s = settings(a)
    quality.log_and_gate(spark, s, quality.silver_checks(spark, s, a.data_year), a.run_id, a.data_year)


def run_gold(spark: SparkSession, a) -> None:
    gold.build_gold(spark, settings(a), a.sql_dir)


def run_check_gold(spark: SparkSession, a) -> None:
    s = settings(a)
    quality.log_and_gate(spark, s, quality.gold_checks(spark, s, a.data_year), a.run_id, a.data_year)
