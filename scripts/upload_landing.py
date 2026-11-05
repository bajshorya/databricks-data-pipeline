"""Copy fetched files and sidecars to the Volume with the Databricks CLI. Files are small, so the CLI is enough."""
import argparse
import subprocess
import sys
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--local-dir", default="data/subset")
    ap.add_argument("--catalog", default="workspace")
    ap.add_argument("--schema", default="partd_ops")
    a = ap.parse_args()
    files = sorted(p for p in Path(a.local_dir).rglob("*") if p.name.endswith((".csv.gz", ".meta.json")))
    if not files:
        print(f"no files under {a.local_dir}", file=sys.stderr)
        return 1
    for f in files:
        rel = f.relative_to(a.local_dir)  # {dataset}/data_year=YYYY/{file}
        dest = f"dbfs:/Volumes/{a.catalog}/{a.schema}/landing/{rel}"
        # `fs cp` does not create parent folders on a Volume.
        subprocess.run(["databricks", "fs", "mkdir", dest.rsplit("/", 1)[0]], check=True)
        subprocess.run(["databricks", "fs", "cp", str(f), dest, "--overwrite"], check=True)
        print("uploaded", dest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
