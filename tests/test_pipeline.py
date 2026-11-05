import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from partd import bronze, pipeline

ROOT = Path(__file__).parent.parent
PCOLS = bronze.load_expected(str(ROOT / "config"), "provider_drug")
GCOLS = bronze.load_expected(str(ROOT / "config"), "geo_drug")


def land(root, dataset, year, cols, rows):
    d = root / dataset / f"data_year={year}"
    d.mkdir(parents=True)
    p = d / "f.csv.gz"
    with gzip.open(p, "wt", encoding="utf-8") as f:
        f.write(",".join(cols) + "\n" + "\n".join(
            ",".join(r.get(c, "") for c in cols) for r in rows) + "\n")
    Path(str(p) + ".meta.json").write_text(json.dumps(
        {"sha256": f"sha{year}{dataset}", "bytes": p.stat().st_size, "file_name": p.name}))


def prov(npi, brand, claims, cost):
    return {"Prscrbr_NPI": npi, "Prscrbr_State_Abrvtn": "RI", "Prscrbr_Type": "Cardiology",
            "Prscrbr_Type_Src": "Medicare", "Prscrbr_City": "Providence", "Brnd_Name": brand,
            "Gnrc_Name": "GEN", "Tot_Clms": claims, "Tot_Drug_Cst": cost, "Tot_30day_Fills": "10",
            "Tot_Day_Suply": "300"}


def geo(level, code, claims):
    return {"Prscrbr_Geo_Lvl": level, "Prscrbr_Geo_Cd": code, "Prscrbr_Geo_Desc": code,
            "Brnd_Name": "DRUGA", "Gnrc_Name": "GEN", "Tot_Prscrbrs": "5", "Tot_Clms": claims,
            "Tot_30day_Fills": "10", "Tot_Drug_Cst": "1000.00", "Opioid_Drug_Flag": "N",
            "Opioid_LA_Drug_Flag": "N", "Antbtc_Drug_Flag": "N", "Antipsych_Drug_Flag": "N"}


@pytest.fixture
def args(tmp_path):
    land(tmp_path, "provider_drug", 2024, PCOLS,
         [prov("1000000001", "DrugA", "50", "500.00"), prov("1000000002", "DrugA", "20", "200.00")])
    land(tmp_path, "geo_drug", 2024, GCOLS, [geo("National", "", "500"), geo("State", "RI", "100")])
    return SimpleNamespace(data_year=2024, force="false", run_id="e2e", catalog="spark_catalog",
                           schema_prefix="e2e", config_dir=str(ROOT / "config"),
                           sql_dir=str(ROOT / "sql"), landing_root=str(tmp_path))


def test_full_chain_and_rerun_is_identical(spark, args):
    for name in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"DROP DATABASE IF EXISTS e2e_{name} CASCADE")

    def run():
        assert pipeline.run_bronze(spark, args) == {"provider_drug": "loaded", "geo_drug": "loaded"} \
            or args.force == "false"
        pipeline.run_silver(spark, args)
        pipeline.run_check_silver(spark, args)
        pipeline.run_gold(spark, args)
        pipeline.run_check_gold(spark, args)
        return {t: spark.table(f"e2e_{t}").count() for t in
                ("silver.prescription", "silver.geo_drug", "gold.drug_year_summary",
                 "gold.reconciliation_year", "ops.dq_results")}

    first = run()
    assert first["silver.prescription"] == 2 and first["gold.drug_year_summary"] == 1
    cov = spark.table("e2e_gold.reconciliation_year").first()
    assert float(cov["claim_coverage"]) == 0.7                       # 70 detail claims / 100 state claims
    args.force = "true"
    second = run()
    first.pop("ops.dq_results"), second.pop("ops.dq_results")        # dq_results is append-only
    assert first == second
