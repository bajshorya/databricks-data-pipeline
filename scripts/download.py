"""Download monthly trip files (and the zone lookup) and upload them to the Databricks Volume.

Each file is downloaded to a temporary folder, uploaded, then deleted, so the laptop keeps nothing.
    python scripts/download.py --months 2024-01,2024-02,2024-03
"""
import argparse
import re
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from sql import ENV, run

BASE = "https://d37ci6vzurychx.cloudfront.net"
TRIPS_URL = BASE + "/trip-data/yellow_tripdata_{month}.parquet"
ZONES_URL = BASE + "/misc/taxi_zone_lookup.csv"


def upload(local: Path, dest_dir: str) -> None:
    # `fs cp` doesn't create folders on a Volume, so make the folder first.
    subprocess.run(["databricks", "fs", "mkdir", dest_dir], check=True, env=ENV)
    subprocess.run(["databricks", "fs", "cp", str(local), f"{dest_dir}/{local.name}", "--overwrite"],
                   check=True, env=ENV)
    print("uploaded", f"{dest_dir}/{local.name}")


def fetch(url: str, folder: Path) -> Path:
    path = folder / url.rsplit("/", 1)[1]
    urllib.request.urlretrieve(url, path)
    return path


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True, help="comma separated YYYY-MM")
    ap.add_argument("--catalog", default="workspace")
    ap.add_argument("--schema", default="pipeline_ops")
    a = ap.parse_args(argv)
    months = a.months.split(",")
    for m in months:
        if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", m):
            ap.error(f"bad month {m!r}")
    run(f"CREATE SCHEMA IF NOT EXISTS {a.catalog}.{a.schema}")
    run(f"CREATE VOLUME IF NOT EXISTS {a.catalog}.{a.schema}.landing")
    root = f"dbfs:/Volumes/{a.catalog}/{a.schema}/landing"
    with tempfile.TemporaryDirectory() as tmp:
        upload(fetch(ZONES_URL, Path(tmp)), f"{root}/zones")
        for m in months:
            f = fetch(TRIPS_URL.format(month=m), Path(tmp))
            upload(f, f"{root}/trips/month={m}")
            f.unlink()  # free disk before the next month
    return 0


if __name__ == "__main__":
    sys.exit(main())
