"""Generate SYNTHETIC Part D files in the CMS layout, for running the pipeline without real data.

Everything here is made up: NPIs, drug names, counts and costs. Real CMS files are blocked from
the author's network (see docs/progress.md). Never present results from this data as real.

Writes gzipped CSVs plus sidecars to {out}/{dataset}/data_year={year}/, the same layout
fetch_cms.py produces, so upload_landing.py and the job work unchanged.
"""
import argparse
import csv
import gzip
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from fetch_cms import write_sidecar

ROOT = Path(__file__).parent.parent
STATES = ["RI", "MA", "CT", "VT", "NH"]
SPECIALTIES = ["Family Practice", "Internal Medicine", "Nurse Practitioner", "Cardiology",
               "Psychiatry", "Physician Assistant", "Dentist", "Orthopedic Surgery",
               "Pain Management", "Endocrinology"]
SYLLABLES = ["zel", "vor", "tam", "quin", "brex", "lor", "dax", "fen", "mira", "ost", "pral", "nux"]


def _load_columns(dataset: str) -> list[str]:
    import tomllib
    with open(ROOT / "config" / "schemas" / f"{dataset}.toml", "rb") as f:
        return tomllib.load(f)["columns"]


def make_drugs(rng: random.Random, n: int) -> list[dict]:
    drugs, seen = [], set()
    while len(drugs) < n:
        generic = "".join(rng.sample(SYLLABLES, 3)) + rng.choice(["ide", "ine", "ate", "ol"])
        if generic in seen:
            continue
        seen.add(generic)
        branded = rng.random() < 0.4  # brand name differs from generic for ~40% of drugs
        brand = (generic[:4] + rng.choice(["ex", "ra", "vo"])).capitalize() if branded else generic
        drugs.append({"brand": brand, "generic": generic, "opioid": rng.random() < 0.08,
                      "la": False, "antibiotic": rng.random() < 0.1,
                      "antipsych": rng.random() < 0.05, "unit_cost": round(rng.uniform(4, 900), 2),
                      "only": rng.choice([None] * 18 + [2022, 2023])})  # a few drugs exist one year only
    for d in drugs:
        d["la"] = d["opioid"] and rng.random() < 0.3
    return drugs


def make_prescribers(rng: random.Random, n: int) -> list[dict]:
    weights = [1, 5, 3, 1, 1]  # rough relative sizes, made up
    return [{"npi": str(9000000000 + i), "state": rng.choices(STATES, weights)[0],
             "specialty": rng.choice(SPECIALTIES), "city": f"Synthetic City {rng.randint(1, 40)}"}
            for i in range(n)]


def provider_rows(rng, prescribers, drugs, year):
    growth = 1.0 if year == 2022 else 1.07
    for p in prescribers:
        for d in rng.sample(drugs, rng.randint(5, 40)):
            if d["only"] not in (None, year):
                continue
            claims = int(rng.lognormvariate(3.2, 0.8) * growth) + 11  # detail file has >= 11 claims
            cost = round(claims * d["unit_cost"] * rng.uniform(0.8, 1.2), 2)
            ge65 = int(claims * rng.uniform(0.5, 1.0))
            suppressed = rng.random() < 0.3
            yield {"Prscrbr_NPI": p["npi"], "Prscrbr_Last_Org_Name": "SYNTHETIC",
                   "Prscrbr_First_Name": "PRESCRIBER", "Prscrbr_City": p["city"],
                   "Prscrbr_State_Abrvtn": p["state"], "Prscrbr_State_FIPS": "00",
                   "Prscrbr_Type": p["specialty"], "Prscrbr_Type_Src": "S",
                   "Brnd_Name": d["brand"], "Gnrc_Name": d["generic"], "Tot_Clms": str(claims),
                   "Tot_30day_Fills": f"{claims * 1.1:.1f}", "Tot_Day_Suply": str(claims * 30),
                   "Tot_Drug_Cst": f"{cost:.2f}",
                   "Tot_Benes": "" if suppressed else str(max(11, claims // 3)),
                   "GE65_Sprsn_Flag": "*" if suppressed else "", "GE65_Tot_Clms": str(ge65),
                   "GE65_Tot_30day_Fills": f"{ge65 * 1.1:.1f}",
                   "GE65_Tot_Drug_Cst": f"{cost * ge65 / claims:.2f}",
                   "GE65_Tot_Day_Suply": str(ge65 * 30), "GE65_Bene_Sprsn_Flag": "*" if suppressed else "",
                   "GE65_Tot_Benes": "" if suppressed else str(max(11, ge65 // 3))}


def corrupt(rng, rows, rate):
    """Plant a known share of bad values, so quarantine and checks have something to catch."""
    kinds = ["invalid_npi", "non_numeric", "negative_cost", "below_11"]
    planted = {k: 0 for k in kinds}
    for r in rows:
        if rng.random() < rate:
            k = rng.choice(kinds)
            planted[k] += 1
            if k == "invalid_npi":
                r["Prscrbr_NPI"] = r["Prscrbr_NPI"][:9]
            elif k == "non_numeric":
                r["Tot_Clms"] = "abc"
            elif k == "negative_cost":
                r["Tot_Drug_Cst"] = "-" + r["Tot_Drug_Cst"]
            else:
                r["Tot_Clms"], r["GE65_Tot_Clms"] = "5", "3"
    return planted


def geo_rows(provider, drugs, year):
    by_state, flags = {}, {d["generic"]: d for d in drugs}
    for r in provider:
        try:
            key = (r["Prscrbr_State_Abrvtn"], r["Brnd_Name"], r["Gnrc_Name"])
            c, cost = int(r["Tot_Clms"]), float(r["Tot_Drug_Cst"])
        except (ValueError, KeyError):
            continue
        agg = by_state.setdefault(key, [0, 0.0, 0])
        agg[0] += c
        agg[1] += max(cost, 0)
        agg[2] += 1

    def row(level, code, desc, brand, generic, claims, cost, prescribers):
        d = flags[generic]
        yn = {True: "Y", False: "N"}
        return {"Prscrbr_Geo_Lvl": level, "Prscrbr_Geo_Cd": code, "Prscrbr_Geo_Desc": desc,
                "Brnd_Name": brand, "Gnrc_Name": generic, "Tot_Prscrbrs": str(prescribers),
                "Tot_Clms": str(claims), "Tot_30day_Fills": f"{claims * 1.1:.1f}",
                "Tot_Drug_Cst": f"{cost:.2f}", "Tot_Benes": "", "GE65_Sprsn_Flag": "*",
                "GE65_Tot_Clms": "", "GE65_Tot_30day_Fills": "", "GE65_Tot_Drug_Cst": "",
                "GE65_Bene_Sprsn_Flag": "*", "GE65_Tot_Benes": "", "LIS_Bene_Cst_Shr": "",
                "NonLIS_Bene_Cst_Shr": "", "Opioid_Drug_Flag": yn[d["opioid"]],
                "Opioid_LA_Drug_Flag": yn[d["la"]], "Antbtc_Drug_Flag": yn[d["antibiotic"]],
                "Antipsych_Drug_Flag": yn[d["antipsych"]]}

    national = {}
    for (state, brand, generic), (c, cost, n) in by_state.items():
        # State totals include small rows the detail file drops, so they run ~8% higher.
        yield row("State", state, state, brand, generic, int(c * 1.08), cost * 1.08, n)
        nat = national.setdefault((brand, generic), [0, 0.0, 0])
        nat[0] += c
        nat[1] += cost
        nat[2] += n
    for (brand, generic), (c, cost, n) in national.items():
        # National covers all states, not only the five synthetic ones.
        yield row("National", "", "National", brand, generic, int(c * 9), cost * 9, n * 9)


def write(out: Path, dataset: str, year: int, cols: list[str], rows: list[dict],
          malformed: int, rng: random.Random) -> Path:
    path = out / dataset / f"data_year={year}" / f"{dataset}_{year}.csv.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
        for _ in range(malformed):
            f.write(",".join(["1"] * (len(cols) - 4)) + "\n")  # short row: lands in _corrupt_record
    write_sidecar(path, dataset, year, "synthetic", STATES)
    return path


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data/synthetic")
    ap.add_argument("--prescribers", type=int, default=4000)
    ap.add_argument("--drugs", type=int, default=600)
    ap.add_argument("--bad-rate", type=float, default=0.003)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--years", default="2022,2023")
    a = ap.parse_args(argv)
    rng = random.Random(a.seed)
    drugs, prescribers = make_drugs(rng, a.drugs), make_prescribers(rng, a.prescribers)
    pcols, gcols = _load_columns("provider_drug"), _load_columns("geo_drug")
    for year in map(int, a.years.split(",")):
        provider = list(provider_rows(rng, prescribers, drugs, year))
        geo = list(geo_rows(provider, drugs, year))  # geo totals come from clean rows
        planted = corrupt(rng, provider, a.bad_rate)
        malformed = max(1, len(provider) // 4000)   # ~0.025%, under the 0.1% threshold
        write(Path(a.out_dir), "provider_drug", year, pcols, provider, malformed, rng)
        write(Path(a.out_dir), "geo_drug", year, gcols, geo, 0, rng)
        print(f"{year}: provider {len(provider) + malformed} rows (planted {planted}, "
              f"malformed {malformed}), geo {len(geo)} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
