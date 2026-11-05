"""Stage 3: print evidence about what Databricks serverless really allows. Run once, read the output."""
import argparse
import os
import sys

ap = argparse.ArgumentParser()
ap.add_argument("--src-path", required=True)
ap.add_argument("--config-dir", required=True)
ap.add_argument("--catalog", default="workspace")
a = ap.parse_args()
sys.path.insert(0, a.src_path)

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
print("python", sys.version.split()[0], "spark", spark.version,
      "ansi", spark.conf.get("spark.sql.ansi.enabled"))
print("catalogs", [r[0] for r in spark.sql("SHOW CATALOGS").collect()])
print("src exists", os.path.isdir(a.src_path), os.listdir(a.src_path))
from partd.config import load_settings

s = load_settings(a.config_dir, a.catalog)
print("config ok", s.table("silver", "prescription"), s.landing_root)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {a.catalog}.{s.ops}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {a.catalog}.{s.ops}.landing")
print("schema and volume created")
t = f"{a.catalog}.{s.ops}.smoke"
df = spark.createDataFrame([(2024, 1)], "data_year INT, v INT")
df.write.format("delta").mode("overwrite").option("replaceWhere", "data_year = 2024") \
    .partitionBy("data_year").saveAsTable(t)
print("delta replaceWhere ok", spark.table(t).count())
spark.sql(f"DROP TABLE {t}")
for f in os.listdir(s.landing_root) if os.path.isdir(s.landing_root) else []:
    print("volume entry", f)
