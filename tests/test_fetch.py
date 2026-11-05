import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
import fetch_cms

PROVIDER = [
    "﻿Prscrbr_NPI,Prscrbr_State_Abrvtn,Tot_Clms,Tot_Day_Suply,GE65_Sprsn_Flag\n",
    "1000000001,RI,20,600,\n",
    "1000000002,CA,30,900,*\n",
    '1000000003,RI,12.5,300,"#"\n',
]


def test_filters_profiles_and_keeps_header(tmp_path):
    out = tmp_path / "p.csv.gz"
    prof = fetch_cms.process(iter(PROVIDER), "provider_drug", {"RI"}, out)
    assert prof["row_count"] == 3 and prof["kept_rows"] == 2 and prof["bom"] is True
    assert prof["header"][0] == "Prscrbr_NPI"          # BOM stripped
    assert prof["rows_per_state"] == {"RI": 2, "CA": 1}
    assert prof["non_whole_number_counts"] == {"Tot_Clms": 1}
    assert prof["null_rate"]["GE65_Sprsn_Flag"] == 1 / 3
    with gzip.open(out, "rt") as f:
        assert "CA" not in f.read()


def test_geo_keeps_national_and_chosen_state(tmp_path):
    lines = iter(["Prscrbr_Geo_Lvl,Prscrbr_Geo_Cd,Prscrbr_Geo_Desc,Tot_Clms\n",
                  "National,,National,100\n", "State,44,RI,10\n", "State,06,CA,20\n"])
    prof = fetch_cms.process(lines, "geo_drug", {"RI"}, tmp_path / "g.csv.gz")
    assert prof["kept_rows"] == 2


def test_sidecar_hash_and_bytes(tmp_path):
    out = tmp_path / "p.csv.gz"
    fetch_cms.process(iter(PROVIDER), "provider_drug", {"RI"}, out)
    meta = fetch_cms.write_sidecar(out, "provider_drug", 2024, "http://x", ["RI"])
    on_disk = json.loads(Path(str(out) + ".meta.json").read_text())
    assert on_disk == meta and meta["bytes"] == out.stat().st_size and len(meta["sha256"]) == 64
