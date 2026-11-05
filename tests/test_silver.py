from pathlib import Path

import pytest
from pyspark.sql import functions as F

from partd import bronze, silver
from partd.config import load_settings

CONFIG = Path(__file__).parent.parent / "config"
PCOLS = bronze.load_expected(str(CONFIG), "provider_drug")
GCOLS = bronze.load_expected(str(CONFIG), "geo_drug")


def frame(spark, cols, rows, year=2024):
    """Bronze-shaped DataFrame: every column a string, plus data_year and _corrupt_record."""
    data = [tuple(r.get(c) for c in cols) + (None, year) for r in rows]
    return spark.createDataFrame(data, ", ".join(f"{c} STRING" for c in cols)
                                 + ", _corrupt_record STRING, data_year INT")


def prov(npi="1000000001", brand="Lipitor", generic="Atorvastatin", claims="20", cost="100.50",
         state="RI", specialty="Cardiology", **kw):
    return {"Prscrbr_NPI": npi, "Prscrbr_Last_Org_Name": "SYNTHETICLAST",
            "Prscrbr_First_Name": "SYNTHETICFIRST", "Prscrbr_City": "Providence",
            "Prscrbr_State_Abrvtn": state, "Prscrbr_Type": specialty, "Prscrbr_Type_Src": "Medicare",
            "Brnd_Name": brand, "Gnrc_Name": generic, "Tot_Clms": claims, "Tot_Drug_Cst": cost,
            "Tot_30day_Fills": "25.0", "Tot_Day_Suply": "600", **kw}


def build(spark, rows):
    return silver.build_provider(frame(spark, PCOLS, rows), "run1")


def test_valid_plus_rejected_equals_input_and_codes(spark):
    rows = [prov(), prov(npi="123"), prov(npi="1000000002", claims="5"),
            prov(npi="1000000003", claims="abc"), prov(npi="1000000004", cost="-1"),
            prov(npi="1000000005", brand="  ", generic=" "),
            prov(npi="1000000006", GE65_Tot_Clms="99")]
    p, _, q = build(spark, rows)
    assert p.count() + q.count() == len(rows) and p.count() == 1
    codes = {c for r in q.collect() for c in r["reject_reasons"]}
    assert {"invalid_npi", "claims_below_11", "non_numeric_total_claims", "negative_cost",
            "missing_drug_name", "ge65_claims_exceed_total"} <= codes


def test_blank_beneficiaries_stay_null_not_rejected(spark):
    p, _, q = build(spark, [prov(Tot_Benes="")])
    assert q.count() == 0 and p.first()["total_beneficiaries"] is None


def test_quarantine_and_silver_have_no_names(spark):
    rows = [prov(npi="bad"), prov()]
    p, py, q = build(spark, rows)
    assert "SYNTHETICLAST" not in "".join(r["raw_row"] for r in q.collect())
    for df in (p, py, q):
        assert not [c for c in df.columns if "name" in c and c not in ("brand_name", "generic_name")]
    corrupt = frame(spark, PCOLS, [prov()]).withColumn("_corrupt_record", F.lit("SYNTHETICLAST,raw line"))
    _, _, q2 = silver.build_provider(corrupt, "r")
    assert "SYNTHETICLAST" not in "".join(r["raw_row"] for r in q2.collect())


def test_grain_unique_and_prescriber_year_picks_top_claims(spark):
    rows = [prov(brand="A", generic="G1", claims="50", state="RI", specialty="Cardiology"),
            prov(brand="B", generic="G2", claims="20", state="MA", specialty="Family Practice")]
    p, py, _ = build(spark, rows)
    assert p.count() == p.dropDuplicates(["data_year", "npi", "brand_name", "generic_name"]).count()
    only = py.collect()
    assert len(only) == 1 and only[0]["state"] == "RI" and only[0]["specialty"] == "Cardiology"


def geo(level="National", brand="Lipitor", generic="Atorvastatin", opioid="N", cost="500.00", **kw):
    return {"Prscrbr_Geo_Lvl": level, "Prscrbr_Geo_Cd": "", "Prscrbr_Geo_Desc": level,
            "Brnd_Name": brand, "Gnrc_Name": generic, "Tot_Prscrbrs": "10", "Tot_Clms": "100",
            "Tot_30day_Fills": "110.0", "Tot_Drug_Cst": cost, "Opioid_Drug_Flag": opioid,
            "Opioid_LA_Drug_Flag": "N", "Antbtc_Drug_Flag": "N", "Antipsych_Drug_Flag": "N", **kw}


def test_geo_flags_national_only_and_invalid_flag_quarantined(spark):
    rows = [geo(opioid="Y", brand="Oxy", generic="Oxycodone"), geo(level="State", opioid="N"),
            geo(brand="Weird", generic="W", opioid="Q")]
    g, dy, q = silver.build_geo(frame(spark, GCOLS, rows), "r")
    assert g.count() == 2 and dy.count() == 1             # state row not in drug_year
    assert dy.first()["is_opioid"] is True
    assert [r["reject_reasons"] for r in q.collect()] == [["invalid_flag"]]


def test_name_differs_from_generic(spark):
    rows = [geo(brand="Lipitor", generic="Atorvastatin"), geo(brand="Metformin", generic="metformin ")]
    _, dy, _ = silver.build_geo(frame(spark, GCOLS, rows), "r")
    got = {r["brand_name"]: r["name_differs_from_generic"] for r in dy.collect()}
    assert got == {"LIPITOR": True, "METFORMIN": False}


@pytest.fixture
def s(spark):
    for layer in ("bronze", "silver"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS sv_{layer}")
    cfg = load_settings(CONFIG, catalog="spark_catalog", schema_prefix="sv")
    return cfg


def test_rerun_replaces_year_without_duplicates(spark, s):
    for ds, cols, rows in (("provider_drug", PCOLS, [prov(), prov(npi="bad")]),
                           ("geo_drug", GCOLS, [geo()])):
        t = s.table("bronze", f"{ds}_raw")
        spark.sql(f"DROP TABLE IF EXISTS {t}")
        frame(spark, cols, rows).write.format("delta").partitionBy("data_year").saveAsTable(t)
    for t in ("prescription", "quarantine", "prescriber_year", "geo_drug", "drug_year"):
        spark.sql(f"DROP TABLE IF EXISTS {s.table('silver', t)}")
    silver.build_silver(spark, s, 2024, "r1")
    silver.build_silver(spark, s, 2024, "r2")
    assert spark.table(s.table("silver", "prescription")).count() == 1
    q = spark.table(s.table("silver", "quarantine"))
    assert q.count() == 1 and q.first()["run_id"] == "r2"
