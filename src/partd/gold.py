from pathlib import Path

from pyspark.sql import SparkSession

from partd.config import Settings

GOLD_TABLES = ["drug_year_summary", "state_year_summary", "specialty_year_summary",
               "reconciliation_year"]


def build_gold(spark: SparkSession, s: Settings, sql_dir: str) -> None:
    """Each SQL file is one SELECT. Rebuilt whole, because YoY values depend on neighbouring years."""
    for name in GOLD_TABLES:
        query = (Path(sql_dir) / "gold" / f"{name}.sql").read_text().format(
            catalog=s.catalog, silver=s.silver, gold=s.gold)
        spark.sql(f"CREATE OR REPLACE TABLE {s.table('gold', name)} USING DELTA AS {query}")
