"""Write docs/metrics.json from real counts. Run with a Spark session that can see the tables.

Usage on Databricks: run as a notebook/job with the same arguments; locally this only counts tests.
"""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent


def test_count() -> int:
    out = subprocess.run([sys.executable, "-m", "pytest", "--collect-only", "-q"], check=False, cwd=ROOT,
                         capture_output=True, text=True).stdout
    return int(out.strip().splitlines()[-1].split()[0])


def table_metrics(spark, catalog: str, prefix: str = "partd") -> dict:
    m = {}
    for layer, tables in {"bronze": ["provider_drug_raw", "geo_drug_raw"],
                          "silver": ["prescription", "prescriber_year", "drug_year", "geo_drug", "quarantine"],
                          "gold": ["drug_year_summary", "state_year_summary", "specialty_year_summary",
                                   "reconciliation_year"]}.items():
        for t in tables:
            m[f"{layer}.{t}"] = spark.table(f"{catalog}.{prefix}_{layer}.{t}").count()
    dq = spark.table(f"{catalog}.{prefix}_ops.dq_results")
    m["dq_distinct_checks"] = dq.select("check_name").distinct().count()
    m["claim_coverage"] = [r.asDict() for r in spark.table(
        f"{catalog}.{prefix}_gold.reconciliation_year").select("data_year", "state", "claim_coverage").collect()]
    return m


if __name__ == "__main__":
    metrics = {"unit_tests": test_count()}
    try:
        from pyspark.sql import SparkSession
        metrics.update(table_metrics(SparkSession.builder.getOrCreate(), "workspace"))
    except Exception as e:  # noqa: BLE001  tables not reachable here: leave numbers out, never invent them
        metrics["table_metrics"] = f"TODO: measured value ({type(e).__name__})"
    (ROOT / "docs").mkdir(exist_ok=True)
    (ROOT / "docs/metrics.json").write_text(json.dumps(metrics, indent=2, default=str))
    print(json.dumps(metrics, indent=2, default=str))
