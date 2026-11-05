import os
import sys

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    # Workers must use the venv Python, or Spark raises PYTHON_VERSION_MISMATCH.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        # Spark's default in-memory catalog fails an internal assertion when Delta updates
        # a partitioned table's metadata on a rewrite. A Hive catalog (Derby, in a temp dir)
        # behaves like a real catalog. Unity Catalog on Databricks is unaffected.
        .config("javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={warehouse}/metastore;create=true")
        .enableHiveSupport()
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
