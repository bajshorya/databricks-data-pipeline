from decimal import Decimal
from pathlib import Path

import pytest

from partd import quality as q
from partd.config import load_settings

ROOT = Path(__file__).parent.parent


@pytest.fixture(scope="module")
def s(spark):
    for layer in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS qt_{layer}")
    return load_settings(ROOT / "config", catalog="spark_catalog", schema_prefix="qt")


def put(spark, s, layer, name, ddl, rows):
    t = s.table(layer, name)
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.createDataFrame(rows, ddl).write.format("delta").saveAsTable(t)


def by_name(checks):
    return {c.name: c for c in checks}


def bronze(spark, s, rows):
    put(spark, s, "bronze", "provider_drug_raw", "data_year INT, _corrupt_record STRING", rows)


def test_bronze_checks_pass_and_fail(spark, s):
    bronze(spark, s, [(2024, None)] * 2000)
    assert all(c.passed for c in q.bronze_checks(spark, s, "provider_drug", 2024))
    bronze(spark, s, [(2024, "bad")] + [(2024, None)] * 99)
    c = by_name(q.bronze_checks(spark, s, "provider_drug", 2024))
    assert not c["bronze_malformed_pct"].passed
    bronze(spark, s, [(2023, None)])
    assert not by_name(q.bronze_checks(spark, s, "provider_drug", 2024))["bronze_row_count_positive"].passed


def silver_tables(spark, s, prescriptions, quarantined, geo_rows=1):
    bronze_n = len(prescriptions) + quarantined
    bronze(spark, s, [(2024, None)] * bronze_n)
    put(spark, s, "bronze", "geo_drug_raw", "data_year INT, _corrupt_record STRING", [(2024, None)] * geo_rows)
    put(spark, s, "silver", "prescription",
        "data_year INT, npi STRING, brand_name STRING, generic_name STRING, state STRING, specialty STRING",
        prescriptions)
    put(spark, s, "silver", "quarantine", "data_year INT, source_table STRING",
        [(2024, "provider_drug")] * quarantined)
    put(spark, s, "silver", "prescriber_year", "data_year INT, npi STRING", [(2024, "1")])
    put(spark, s, "silver", "drug_year", "data_year INT, brand_name STRING, generic_name STRING",
        [(2024, "A", "G")])
    put(spark, s, "silver", "geo_drug",
        "data_year INT, geo_level STRING, geo_code STRING, brand_name STRING, generic_name STRING",
        [(2024, "National", "", "A", "G")] * geo_rows if geo_rows == 1 else [])


GOOD = [(2024, "1", "A", "G", "RI", "Cardio")]


def test_silver_checks_all_pass_on_clean_data(spark, s):
    silver_tables(spark, s, GOOD * 1, 0)
    c = q.silver_checks(spark, s, 2024)
    assert all(x.passed for x in c), [x for x in c if not x.passed]


def test_quarantine_rate_conservation_and_grain_fail(spark, s):
    silver_tables(spark, s, GOOD, 5)                       # 5 of 6 rows quarantined
    assert not by_name(q.silver_checks(spark, s, 2024))["quarantine_pct_provider_drug"].passed
    silver_tables(spark, s, GOOD * 2, 0)                   # duplicate grain
    c = by_name(q.silver_checks(spark, s, 2024))
    assert not c["grain_unique_prescription"].passed
    silver_tables(spark, s, GOOD, 0)
    bronze(spark, s, [(2024, None)] * 3)                   # bronze has 2 rows silver can't account for
    assert not by_name(q.silver_checks(spark, s, 2024))["valid_plus_quarantine_equals_bronze_provider_drug"].passed


def test_warn_checks_fail_but_do_not_block(spark, s):
    silver_tables(spark, s, [(2024, "1", "A", "G", "RI", "Cardio"),
                             (2024, "1", "A", "H", "MA", "Cardio")], 0)   # two states, drug H unmatched
    checks = q.silver_checks(spark, s, 2024)
    c = by_name(checks)
    assert not c["prescriber_single_state_specialty"].passed and not c["unmatched_drug_pct"].passed
    q.log_and_gate(spark, s, [x for x in checks if x.severity == "warn"], "r", 2024)  # no raise


def gold(spark, s, state_cost, spec_cost, detail=10, geo=20):
    put(spark, s, "silver", "prescription", "data_year INT, total_drug_cost DECIMAL(18,2)",
        [(2024, Decimal("100.00"))])
    for n, v in (("state_year_summary", state_cost), ("specialty_year_summary", spec_cost)):
        put(spark, s, "gold", n, "data_year INT, total_drug_cost DECIMAL(18,2)",
            [(2024, Decimal(v))])
    put(spark, s, "gold", "reconciliation_year", "data_year INT, detail_claims BIGINT, geo_claims BIGINT",
        [(2024, detail, geo)])


def test_gold_checks_pass_and_fail(spark, s):
    gold(spark, s, "100.00", "100.00")
    assert all(c.passed for c in q.gold_checks(spark, s, 2024))
    gold(spark, s, "99.00", "100.00", detail=30, geo=20)
    failed = {c.name for c in q.gold_checks(spark, s, 2024) if not c.passed}
    assert failed == {"state_year_summary_cost_equals_silver", "detail_claims_within_geo_claims"}


def test_all_results_logged_before_blocking_raise(spark, s):
    spark.sql(f"DROP TABLE IF EXISTS {s.table('ops', 'dq_results')}")
    checks = [q.Check("a", "silver", "t", 1.0, 0.0, "block", False),
              q.Check("b", "silver", "t", 0.0, 0.0, "block", True),
              q.Check("c", "silver", "t", 1.0, 0.0, "block", False)]
    with pytest.raises(q.QualityGateError, match="'a', 'c'"):
        q.log_and_gate(spark, s, checks, "run9", 2024)
    got = {r["check_name"]: r["status"] for r in spark.table(s.table("ops", "dq_results")).collect()}
    assert got == {"a": "fail", "b": "pass", "c": "fail"}
