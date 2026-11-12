from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.config import Settings, check_month


def write_month(df: DataFrame, table: str, month: str) -> None:
    """Replace one month of a table and leave other months alone, so a rerun never duplicates."""
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", f"month = '{check_month(month)}'")
       .partitionBy("month").saveAsTable(table))


def as_strings(df: DataFrame) -> DataFrame:
    """Bronze keeps values as received, as text with lowercase names. Types are fixed in Silver,
    so a month whose file uses a different type or name case still loads."""
    return df.select([F.col(f"`{c}`").cast("string").alias(c.lower()) for c in df.columns])


def load_trips(spark: SparkSession, s: Settings, month: str, landing_root: str) -> None:
    path = f"{landing_root}/trips/month={check_month(month)}"
    df = (as_strings(spark.read.parquet(path))
          .withColumn("source_file", F.col("_metadata.file_path"))
          .withColumn("ingested_at", F.current_timestamp())
          .withColumn("month", F.lit(month)))
    write_month(df, s.table("bronze", "trips_raw"), month)


def load_zones(spark: SparkSession, s: Settings, landing_root: str) -> None:
    """The zone lookup is one small file that isn't split by month: replace it whole."""
    df = (spark.read.option("header", True).csv(f"{landing_root}/zones")
          .withColumn("ingested_at", F.current_timestamp()))
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
        .saveAsTable(s.table("bronze", "zones_raw"))
