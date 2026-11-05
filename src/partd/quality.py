from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from partd.config import Settings


class QualityGateError(Exception):
    pass


@dataclass(frozen=True)
class Check:
    name: str
    layer: str
    table_name: str
    observed: float
    threshold: float
    severity: str  # "block" or "warn"
    passed: bool


def _n(spark: SparkSession, query: str) -> float:
    return float(spark.sql(query).first()[0] or 0)


def _check(name, layer, table, observed, threshold, severity, passed) -> Check:
    return Check(name, layer, table, float(observed), float(threshold), severity, bool(passed))


def log_and_gate(spark: SparkSession, s: Settings, checks: list[Check], run_id: str,
                 data_year: int) -> None:
    """Log every result first. Only then raise, so one failure never hides the others."""
    rows = [(run_id, c.name, c.layer, c.table_name, data_year, c.observed, c.threshold,
             c.severity, "pass" if c.passed else "fail") for c in checks]
    df = spark.createDataFrame(
        rows, "run_id STRING, check_name STRING, layer STRING, table_name STRING, data_year INT, "
              "observed_value DOUBLE, threshold DOUBLE, severity STRING, status STRING"
    ).withColumn("checked_at", F.current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(s.table("ops", "dq_results"))
    failed = [c.name for c in checks if not c.passed and c.severity == "block"]
    if failed:
        raise QualityGateError(f"blocking checks failed: {failed}")


def bronze_checks(spark: SparkSession, s: Settings, dataset: str, year: int) -> list[Check]:
    t = s.table("bronze", f"{dataset}_raw")
    r = spark.sql(f"SELECT COUNT(*) n, SUM(CASE WHEN _corrupt_record IS NOT NULL THEN 1 ELSE 0 END) bad "
                  f"FROM {t} WHERE data_year = {year}").first()
    n, bad = r["n"], r["bad"] or 0
    pct = 100 * bad / n if n else 0
    return [_check("bronze_row_count_positive", "bronze", t, n, 0, "block", n > 0),
            _check("bronze_malformed_pct", "bronze", t, pct, s.malformed_max_pct, "block",
                   pct < s.malformed_max_pct)]


GRAINS = {"prescription": "data_year, npi, brand_name, generic_name",
          "prescriber_year": "data_year, npi",
          "drug_year": "data_year, brand_name, generic_name",
          "geo_drug": "data_year, geo_level, geo_code, brand_name, generic_name"}


def silver_checks(spark: SparkSession, s: Settings, year: int) -> list[Check]:
    out = []
    q = s.table("silver", "quarantine")
    for ds, silver_name in (("provider_drug", "prescription"), ("geo_drug", "geo_drug")):
        bronze_n = _n(spark, f"SELECT COUNT(*) FROM {s.table('bronze', ds + '_raw')} WHERE data_year={year}")
        silver_n = _n(spark, f"SELECT COUNT(*) FROM {s.table('silver', silver_name)} WHERE data_year={year}")
        quar_n = _n(spark, f"SELECT COUNT(*) FROM {q} WHERE data_year={year} AND source_table='{ds}'")
        pct = 100 * quar_n / bronze_n if bronze_n else 0
        out.append(_check(f"quarantine_pct_{ds}", "silver", q, pct, s.quarantine_max_pct, "block",
                          pct < s.quarantine_max_pct))
        diff = bronze_n - silver_n - quar_n
        out.append(_check(f"valid_plus_quarantine_equals_bronze_{ds}", "silver", q, diff, 0, "block",
                          diff == 0))
    for name, grain in GRAINS.items():
        t = s.table("silver", name)
        dups = _n(spark, f"SELECT COUNT(*) FROM (SELECT 1 FROM {t} WHERE data_year={year} "
                         f"GROUP BY {grain} HAVING COUNT(*) > 1)")
        out.append(_check(f"grain_unique_{name}", "silver", t, dups, 0, "block", dups == 0))
    p, d = s.table("silver", "prescription"), s.table("silver", "drug_year")
    total = _n(spark, f"SELECT COUNT(*) FROM {p} WHERE data_year={year}")
    miss = _n(spark, f"SELECT COUNT(*) FROM {p} p LEFT ANTI JOIN {d} d ON p.data_year=d.data_year "
                     f"AND p.brand_name=d.brand_name AND p.generic_name=d.generic_name "
                     f"WHERE p.data_year={year}")
    pct = 100 * miss / total if total else 0
    out.append(_check("unmatched_drug_pct", "silver", p, pct, s.unmatched_drug_max_pct, "warn",
                      pct < s.unmatched_drug_max_pct))
    multi = _n(spark, f"SELECT COUNT(*) FROM (SELECT npi FROM {p} WHERE data_year={year} GROUP BY npi "
                      f"HAVING COUNT(DISTINCT state) > 1 OR COUNT(DISTINCT specialty) > 1)")
    out.append(_check("prescriber_single_state_specialty", "silver", p, multi, 0, "warn", multi == 0))
    return out


def gold_checks(spark: SparkSession, s: Settings, year: int) -> list[Check]:
    p = s.table("silver", "prescription")
    silver_cost = _n(spark, f"SELECT SUM(total_drug_cost) FROM {p} WHERE data_year={year}")
    out = []
    for name in ("state_year_summary", "specialty_year_summary"):
        t = s.table("gold", name)
        cost = _n(spark, f"SELECT SUM(total_drug_cost) FROM {t} WHERE data_year={year}")
        out.append(_check(f"{name}_cost_equals_silver", "gold", t, cost - silver_cost, 0, "block",
                          cost == silver_cost))
    r = s.table("gold", "reconciliation_year")
    over = _n(spark, f"SELECT COUNT(*) FROM {r} WHERE data_year={year} AND detail_claims > geo_claims")
    out.append(_check("detail_claims_within_geo_claims", "gold", r, over, 0, "block", over == 0))
    return out
