from pathlib import Path

from pyspark.sql import SparkSession

from pipeline.config import Settings

GOLD_TABLES = ["daily_summary", "zone_hour_earnings", "shift_best_zones", "airport_vs_city"]


def build_gold(spark: SparkSession, s: Settings, sql_dir: str) -> None:
    """Each SQL file is one SELECT. Rebuilt whole, because the moving average and the
    month-over-month change depend on neighbouring months."""
    for name in GOLD_TABLES:
        query = (Path(sql_dir) / "gold" / f"{name}.sql").read_text().format(
            catalog=s.catalog, silver=s.silver)
        spark.sql(f"CREATE OR REPLACE TABLE {s.table('gold', name)} USING DELTA AS {query}")
