from decimal import Decimal
from pathlib import Path

import pytest

from partd.config import load_settings
from partd.gold import build_gold

ROOT = Path(__file__).parent.parent
SQL = ROOT / "sql"

# Hand-computed fixture (data_year, npi, state, specialty, brand, generic, claims, cost):
#  2023  A  npi1 RI Cardio 100 1000.00 | A npi2 RI Cardio 50 500.00 -> A 2023 cost 1500, 150 claims, 2 prescribers
#        C  npi1 RI Cardio 10 100.00
#  2024  A  npi1 RI Cardio 100 3000.00 -> A cost 3000, cost/claim 30, YoY +1500 (+100%), rank 1
#        B  npi1 RI Cardio 20 200.00   -> no 2023 row, YoY NULL. B is an opioid in 2024
#        Z  npi1 RI Cardio 0 50.00     -> zero claims, cost/claim NULL
#        D  npi3 MA Family 40 400.00   -> no drug_year row: counted in totals, not as opioid
#  2025  C  npi1 RI Cardio 10 400.00   -> 2024 missing, so YoY must be NULL (gap year)
#  Cost ranks 2024: A 1, D 2, B 3, Z 4. RI 2024: 120 claims, 3250.00, opioid 20 -> share 1/6. MA: 40, 400.00, share 0.
#  Geo state rows 2024: RI claims 240 cost 6500.00 -> coverage 0.5; MA claims 40 cost 400.00 -> 1.0


@pytest.fixture(scope="module")
def gold(spark):
    for layer in ("silver", "gold"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS gd_{layer}")
    s = load_settings(ROOT / "config", catalog="spark_catalog", schema_prefix="gd")
    rows = [(2023, "1", "RI", "Cardio", "A", "G", 100, "1000.00"),
            (2023, "2", "RI", "Cardio", "A", "G", 50, "500.00"),
            (2023, "1", "RI", "Cardio", "C", "G", 10, "100.00"),
            (2024, "1", "RI", "Cardio", "A", "G", 100, "3000.00"),
            (2024, "1", "RI", "Cardio", "B", "G", 20, "200.00"),
            (2024, "1", "RI", "Cardio", "Z", "G", 0, "50.00"),
            (2024, "3", "MA", "Family", "D", "G", 40, "400.00"),
            (2025, "1", "RI", "Cardio", "C", "G", 10, "400.00")]
    df = spark.createDataFrame(rows, "data_year INT, npi STRING, state STRING, specialty STRING, "
                                     "brand_name STRING, generic_name STRING, total_claims BIGINT, c STRING")
    df.selectExpr("*", "CAST(c AS DECIMAL(18,2)) AS total_drug_cost").drop("c") \
        .write.format("delta").mode("overwrite").saveAsTable(s.table("silver", "prescription"))
    spark.createDataFrame([(2024, "A", "G", False), (2024, "B", "G", True), (2024, "Z", "G", False)],
                          "data_year INT, brand_name STRING, generic_name STRING, is_opioid BOOLEAN") \
        .write.format("delta").mode("overwrite").saveAsTable(s.table("silver", "drug_year"))
    # geo_code = state abbreviation is UNVERIFIED until real data is read.
    spark.createDataFrame(
        [(2024, "State", "RI", 240, "6500.00"), (2024, "State", "MA", 40, "400.00")],
        "data_year INT, geo_level STRING, geo_code STRING, total_claims BIGINT, c STRING") \
        .selectExpr("*", "CAST(c AS DECIMAL(18,2)) AS total_drug_cost").drop("c") \
        .write.format("delta").mode("overwrite").saveAsTable(s.table("silver", "geo_drug"))
    build_gold(spark, s, str(SQL))
    return lambda name: spark.table(s.table("gold", name))


def row(df, **where):
    return df.where(" AND ".join(f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}"
                                 for k, v in where.items())).first()


def test_drug_cost_per_claim_yoy_and_rank(gold):
    d = gold("drug_year_summary")
    a = row(d, data_year=2024, brand_name="A")
    assert a["cost_per_claim"] == Decimal(30) and a["cost_rank"] == 1
    assert a["yoy_cost_change"] == Decimal("1500.00") and a["yoy_cost_change_pct"] == 1
    assert a["prescriber_count"] == 1
    assert row(d, data_year=2024, brand_name="D")["cost_rank"] == 2


def test_missing_prior_and_gap_year_give_null_yoy(gold):
    d = gold("drug_year_summary")
    assert row(d, data_year=2024, brand_name="B")["yoy_cost_change"] is None
    assert row(d, data_year=2025, brand_name="C")["yoy_cost_change_pct"] is None


def test_zero_claims_gives_null_not_error(gold):
    assert row(gold("drug_year_summary"), data_year=2024, brand_name="Z")["cost_per_claim"] is None


def test_state_opioid_share_and_unmatched_drug(gold):
    st = gold("state_year_summary")
    ri, ma = row(st, data_year=2024, state="RI"), row(st, data_year=2024, state="MA")
    assert ri["total_claims"] == 120 and ri["total_drug_cost"] == Decimal("3250.00")
    assert ri["opioid_claims"] == 20 and round(ri["opioid_claim_share"], 4) == round(1 / 6, 4)
    assert ma["total_claims"] == 40 and ma["opioid_claims"] == 0   # D has no drug_year row


def test_specialty_totals_equal_silver(gold):
    sp = gold("specialty_year_summary")
    assert row(sp, data_year=2024, specialty="Cardio")["total_drug_cost"] == Decimal("3250.00")
    assert row(sp, data_year=2024, specialty="Cardio")["yoy_cost_change_pct"] is not None


def test_coverage_per_state(gold):
    rec = gold("reconciliation_year")
    assert row(rec, data_year=2024, state="RI")["claim_coverage"] == Decimal("0.5") or \
        float(row(rec, data_year=2024, state="RI")["claim_coverage"]) == 0.5
    assert float(row(rec, data_year=2024, state="MA")["cost_coverage"]) == 1.0


def test_no_beneficiary_sum_in_gold_sql():
    for f in (SQL / "gold").glob("*.sql"):
        assert "total_beneficiaries" not in f.read_text(), f.name
