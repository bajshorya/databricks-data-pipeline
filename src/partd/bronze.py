import codecs
import csv
import gzip
import hashlib
import json
import re
import tomllib
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

MANIFEST_COLS = ("dataset", "data_year", "file_name", "file_sha256", "file_bytes",
                 "row_count", "loaded_at")


class BronzeError(Exception):
    pass


def read_header(path: str) -> list[str]:
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rb") as raw:
        text = codecs.getreader("utf-8-sig")(raw)  # utf-8-sig drops a BOM from the first name
        return next(csv.reader(text))


def check_columns(header: list[str], expected: list[str]) -> None:
    missing, unknown = set(expected) - set(header), set(header) - set(expected)
    if missing or unknown:
        raise BronzeError(f"header mismatch: missing={sorted(missing)} unknown={sorted(unknown)}")


def load_expected(config_dir: str, dataset: str) -> list[str]:
    with open(Path(config_dir) / "schemas" / f"{dataset}.toml", "rb") as f:
        return tomllib.load(f)["columns"]


def check_sidecar(csv_path: str) -> dict:
    """Landing check: the sidecar exists and its byte count matches the real file."""
    side = Path(csv_path + ".meta.json")
    if not side.exists():
        raise BronzeError(f"missing sidecar {side}")
    meta = json.loads(side.read_text())
    actual = Path(csv_path).stat().st_size
    if meta["bytes"] != actual:
        raise BronzeError(f"size mismatch: sidecar {meta['bytes']} vs file {actual}")
    return meta


def year_from_path(path: str) -> int:
    m = re.search(r"data_year=(\d{4})", path)
    if not m:
        raise BronzeError(f"no data_year in path {path}")
    return int(m.group(1))


def read_raw(spark: SparkSession, csv_path: str, header: list[str]) -> DataFrame:
    schema = StructType([StructField(c, StringType()) for c in header]
                        + [StructField("_corrupt_record", StringType())])
    return (spark.read.format("csv").schema(schema)
            .option("header", True).option("mode", "PERMISSIVE")
            .option("quote", '"').option("escape", '"')
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .load(csv_path))


def write_year(df: DataFrame, table: str, data_year: int, extra: str = "") -> None:
    """Replace one year of a table, leaving other years untouched. `extra` narrows the replace."""
    where = f"data_year = {data_year}" + (f" AND {extra}" if extra else "")
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", where)
       .partitionBy("data_year").saveAsTable(table))


def already_loaded(spark: SparkSession, manifest: str, dataset: str, year: int, sha: str) -> bool:
    if not spark.catalog.tableExists(manifest):
        return False
    return spark.table(manifest).where(
        (F.col("dataset") == dataset) & (F.col("data_year") == year)
        & (F.col("file_sha256") == sha)).limit(1).count() > 0


def load_bronze(spark: SparkSession, csv_path: str, dataset: str, data_year: int,
                config_dir: str, bronze_schema: str, force: bool = False) -> str:
    """Load one file. Returns 'loaded' or 'skipped'. Raises before any write on bad input."""
    meta = check_sidecar(csv_path)
    if year_from_path(csv_path) != data_year:
        raise BronzeError("year in path differs from job parameter")
    header = read_header(csv_path)
    check_columns(header, load_expected(config_dir, dataset))
    manifest = f"{bronze_schema}.file_manifest"
    if not force and already_loaded(spark, manifest, dataset, data_year, meta["sha256"]):
        return "skipped"

    df = (read_raw(spark, csv_path, header)
          .withColumn("data_year", F.lit(data_year))
          .withColumn("source_file", F.col("_metadata.file_path"))
          .withColumn("file_sha256", F.lit(meta["sha256"]))
          .withColumn("ingested_at", F.current_timestamp()))
    write_year(df, f"{bronze_schema}.{dataset}_raw", data_year)
    # Manifest is updated only after the table write succeeded.
    n = spark.table(f"{bronze_schema}.{dataset}_raw").where(f"data_year = {data_year}").count()
    row = spark.createDataFrame(
        [(dataset, data_year, meta["file_name"], meta["sha256"], meta["bytes"], n)],
        "dataset STRING, data_year INT, file_name STRING, file_sha256 STRING, "
        "file_bytes BIGINT, row_count BIGINT").withColumn("loaded_at", F.current_timestamp())
    row.createOrReplaceTempView("_new_manifest")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {manifest} USING DELTA AS SELECT * FROM _new_manifest WHERE 1=0")
    spark.sql(f"""MERGE INTO {manifest} t USING _new_manifest s
                  ON t.dataset = s.dataset AND t.data_year = s.data_year
                  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    return "loaded"


def sha256_file(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()
