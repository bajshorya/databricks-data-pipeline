"""Stream a CMS Part D CSV, keep only the chosen states, profile the whole file, write a sidecar.

No full CMS file is ever saved: rows are filtered as they stream past.
"""
import argparse
import codecs
import csv
import gzip
import hashlib
import json
import sys
import tomllib
import urllib.request
from collections import Counter
from pathlib import Path

STATE_COL = {"provider_drug": "Prscrbr_State_Abrvtn", "geo_drug": "Prscrbr_Geo_Desc"}
LEVEL_COL = "Prscrbr_Geo_Lvl"
CODE_COL = "Prscrbr_Geo_Cd"
WHOLE_NUMBER_COLS = ("Tot_Clms", "Tot_Day_Suply", "GE65_Tot_Clms")
FLAG_COLS = ("GE65_Sprsn_Flag", "GE65_Bene_Sprsn_Flag", "Opioid_Drug_Flag",
             "Opioid_LA_Drug_Flag", "Antbtc_Drug_Flag", "Antipsych_Drug_Flag", LEVEL_COL)
USER_AGENT = "partd-lakehouse-student-project"


def keep_row(dataset: str, row: dict, states: set[str]) -> bool:
    if dataset == "provider_drug":
        return row[STATE_COL[dataset]] in states
    level = row.get(LEVEL_COL, "")
    if level == "National":
        return True
    # shortcut: state match on code or description, until Stage 2 confirms which holds the abbreviation
    return level == "State" and (row.get(CODE_COL) in states or row.get("Prscrbr_Geo_Desc") in states)


def process(lines, dataset: str, states: set[str], out_path: Path) -> dict:
    """Filter and profile one stream of CSV lines. Returns the profile."""
    first = next(lines)
    bom = first.startswith("﻿")
    reader = csv.DictReader(_chain(first.lstrip("﻿"), lines))
    header = reader.fieldnames
    nulls, flags, state_rows = Counter(), {c: Counter() for c in FLAG_COLS}, Counter()
    non_whole, total, kept = Counter(), 0, 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out_path, "wt", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=header)
        writer.writeheader()
        for row in reader:
            total += 1
            for col, val in row.items():
                if val is None or val.strip() == "":
                    nulls[col] += 1
            for col in FLAG_COLS:
                if col in row and len(flags[col]) < 50:
                    flags[col][row[col]] += 1
            for col in WHOLE_NUMBER_COLS:
                v = (row.get(col) or "").strip()
                if v and not v.isdigit():
                    non_whole[col] += 1
            if dataset == "provider_drug":
                state_rows[row[STATE_COL[dataset]]] += 1
            if keep_row(dataset, row, states):
                writer.writerow(row)
                kept += 1
    return {"dataset": dataset, "row_count": total, "kept_rows": kept, "header": header,
            "bom": bom, "null_rate": {c: nulls[c] / total for c in header} if total else {},
            "values": {c: dict(v) for c, v in flags.items() if v},
            "non_whole_number_counts": dict(non_whole), "rows_per_state": dict(state_rows)}


def _chain(first, rest):
    yield first
    yield from rest


def write_sidecar(gz_path: Path, dataset: str, year: int, url: str, states: list[str]) -> dict:
    sha = hashlib.sha256(gz_path.read_bytes()).hexdigest()
    meta = {"dataset": dataset, "data_year": year, "file_name": gz_path.name, "sha256": sha,
            "bytes": gz_path.stat().st_size, "source_url": url, "states": states}
    Path(str(gz_path) + ".meta.json").write_text(json.dumps(meta, indent=2))
    return meta


def stream_lines(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        yield from codecs.iterdecode(resp, "utf-8", errors="replace")


def local_lines(path: str):
    # newline="" keeps embedded newlines inside quoted fields intact for the csv module
    with open(path, encoding="utf-8", newline="") as f:
        yield from f


def head(url: str) -> int:
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return int(resp.headers.get("Content-Length", 0))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True, choices=list(STATE_COL))
    ap.add_argument("--year", required=True, type=int)
    ap.add_argument("--states", required=True, help="comma separated, e.g. RI,MA")
    ap.add_argument("--out-dir", default="data/subset")
    ap.add_argument("--head-only", action="store_true")
    ap.add_argument("--file", help="read a CSV you downloaded in a browser, instead of the network")
    a = ap.parse_args(argv)
    with open(Path(__file__).parent.parent / "config/sources.toml", "rb") as f:
        url = tomllib.load(f)[a.dataset][str(a.year)]
    if not a.file:
        size = head(url)
        print(f"{a.dataset} {a.year}: {size / 1e9:.2f} GB")
        if a.head_only:
            return 0
    states = a.states.split(",")
    out = Path(a.out_dir) / a.dataset / f"data_year={a.year}" / f"{a.dataset}_{a.year}.csv.gz"
    lines = local_lines(a.file) if a.file else stream_lines(url)
    profile = process(lines, a.dataset, set(states), out)
    Path(str(out) + ".profile.json").write_text(json.dumps(profile, indent=2))
    meta = write_sidecar(out, a.dataset, a.year, url, states)
    print(f"read {profile['row_count']} rows, kept {profile['kept_rows']}, wrote {meta['bytes']} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
