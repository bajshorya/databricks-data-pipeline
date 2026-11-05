"""Write docs/metrics.json from real counts, queried through the Databricks SQL warehouse.

Every number in the README and resume must come from this file. Never edit it by hand.
"""
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from sql import run

ROOT = Path(__file__).parent.parent
C = "workspace"


def test_count() -> int:
    out = subprocess.run([sys.executable, "-m", "pytest", "--collect-only", "-q"], check=False,
                         cwd=ROOT, capture_output=True, text=True).stdout
    return int(out.strip().splitlines()[-1].split()[0])


def scalar(q: str):
    return run(q)[0][0]


def main() -> int:
    tables = {"bronze": ["provider_drug_raw", "geo_drug_raw"],
              "silver": ["prescription", "prescriber_year", "drug_year", "geo_drug", "quarantine"],
              "gold": ["drug_year_summary", "state_year_summary", "specialty_year_summary",
                       "reconciliation_year"]}
    m = {"data": "SYNTHETIC (CMS layout). Real CMS files were not reachable; see docs/progress.md",
         "unit_tests": test_count(), "rows": {}}
    for layer, names in tables.items():
        for t in names:
            m["rows"][f"{layer}.{t}"] = int(scalar(f"SELECT COUNT(*) FROM {C}.partd_{layer}.{t}"))
    m["years"] = [int(r[0]) for r in run(
        f"SELECT DISTINCT data_year FROM {C}.partd_silver.prescription ORDER BY 1")]
    m["states"] = int(scalar(f"SELECT COUNT(DISTINCT state) FROM {C}.partd_silver.prescription"))
    m["gold_tables"] = len(tables["gold"])
    m["distinct_dq_checks"] = int(scalar(
        f"SELECT COUNT(DISTINCT check_name) FROM {C}.partd_ops.dq_results"))
    m["quarantine_pct"] = round(100 * m["rows"]["silver.quarantine"] / m["rows"]["bronze.provider_drug_raw"], 3)
    m["claim_coverage_min_max"] = [float(x) for x in run(
        f"SELECT ROUND(MIN(claim_coverage),4), ROUND(MAX(claim_coverage),4) "
        f"FROM {C}.partd_gold.reconciliation_year")[0]]
    (ROOT / "docs/metrics.json").write_text(json.dumps(m, indent=2))
    print(json.dumps(m, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
