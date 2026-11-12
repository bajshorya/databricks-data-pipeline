"""Run one SQL statement on a Databricks SQL warehouse and print the rows. Used to check results."""
import json
import os
import subprocess
import sys

WAREHOUSE = "1077001b73e3617a"  # Serverless Starter Warehouse
# Outside the bundle folder the CLI needs the host and the saved login profile named explicitly.
ENV = {**os.environ, "DATABRICKS_HOST": "https://dbc-c6a80f71-6f05.cloud.databricks.com",
       "DATABRICKS_CONFIG_PROFILE": "DEFAULT"}


def run(statement: str) -> list[list]:
    body = json.dumps({"warehouse_id": WAREHOUSE, "statement": statement, "wait_timeout": "50s"})
    out = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--json", body],
                         check=True, capture_output=True, text=True, env=ENV).stdout
    r = json.loads(out)
    if r["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(r["status"])
    cols = [c["name"] for c in r["manifest"]["schema"].get("columns", [])]  # DDL has none
    rows = r.get("result", {}).get("data_array", [])
    print(" | ".join(cols))
    for row in rows:
        print(" | ".join("NULL" if v is None else str(v) for v in row))
    return rows


if __name__ == "__main__":
    run(sys.argv[1])
