import gzip
import json
from pathlib import Path

import pytest

from partd import bronze

CONFIG = str(Path(__file__).parent.parent / "config")
HEADER = bronze.load_expected(CONFIG, "provider_drug")


def make_file(tmp_path, year=2024, rows=None, header=None, bom=False, name="a"):
    header = header or HEADER
    d = tmp_path / name / "provider_drug" / f"data_year={year}"
    d.mkdir(parents=True)
    p = d / "f.csv.gz"
    body = rows if rows is not None else [
        ",".join(["1000000001"] + ["x"] * (len(header) - 1)),
        ",".join(["1000000002", '"Smith, J"'] + [""] * (len(header) - 2)),
    ]
    text = ("﻿" if bom else "") + ",".join(header) + "\n" + "\n".join(body) + "\n"
    with gzip.open(p, "wt", encoding="utf-8") as f:
        f.write(text)
    meta = {"sha256": "h" + str(len(body)) + name, "bytes": p.stat().st_size, "file_name": "f.csv.gz"}
    Path(str(p) + ".meta.json").write_text(json.dumps(meta))
    return str(p)


@pytest.fixture
def schema(spark):
    s = "bz"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {s}")
    for t in ("provider_drug_raw", "file_manifest"):
        spark.sql(f"DROP TABLE IF EXISTS {s}.{t}")
    return s


def load(spark, path, schema, year=2024, force=False):
    return bronze.load_bronze(spark, path, "provider_drug", year, CONFIG, schema, force)


def test_all_strings_and_quoted_comma(spark, tmp_path, schema):
    load(spark, make_file(tmp_path), schema)
    df = spark.table(f"{schema}.provider_drug_raw")
    assert all(t == "string" for c, t in df.dtypes
               if c not in ("data_year", "ingested_at"))
    assert df.where("Prscrbr_NPI = '1000000002'").first()["Prscrbr_Last_Org_Name"] == "Smith, J"


def test_bom_does_not_change_first_column(tmp_path):
    assert bronze.read_header(make_file(tmp_path, bom=True))[0] == "Prscrbr_NPI"


def test_short_and_long_rows_are_corrupt(spark, tmp_path, schema):
    n = len(HEADER)
    rows = [",".join(["1"] * n), ",".join(["2"] * (n - 3)), ",".join(["3"] * (n + 3))]
    load(spark, make_file(tmp_path, rows=rows), schema)
    df = spark.table(f"{schema}.provider_drug_raw")
    assert df.where("_corrupt_record IS NOT NULL").count() == 2


def test_missing_or_unknown_column_raises_before_write(spark, tmp_path, schema):
    p = make_file(tmp_path, header=HEADER[:-1], rows=["1," + ",".join(["x"] * (len(HEADER) - 2))])
    with pytest.raises(bronze.BronzeError, match="missing"):
        load(spark, p, schema)
    assert not spark.catalog.tableExists(f"{schema}.provider_drug_raw")


def test_year_mismatch_raises(spark, tmp_path, schema):
    with pytest.raises(bronze.BronzeError, match="year"):
        load(spark, make_file(tmp_path, year=2023), schema, year=2024)


def test_missing_sidecar_and_byte_mismatch_raise(spark, tmp_path, schema):
    p = make_file(tmp_path)
    meta = json.loads(Path(p + ".meta.json").read_text())
    meta["bytes"] += 1
    Path(p + ".meta.json").write_text(json.dumps(meta))
    with pytest.raises(bronze.BronzeError, match="size"):
        load(spark, p, schema)
    Path(p + ".meta.json").unlink()
    with pytest.raises(bronze.BronzeError, match="sidecar"):
        load(spark, p, schema)


def test_idempotent_skip_force_and_year_isolation(spark, tmp_path, schema):
    p24 = make_file(tmp_path, 2024, name="a")
    assert load(spark, p24, schema) == "loaded"
    assert load(spark, p24, schema) == "skipped"          # unchanged file
    assert load(spark, p24, schema, force=True) == "loaded"
    t = f"{schema}.provider_drug_raw"
    assert spark.table(t).count() == 2                      # no duplicates after reload
    load(spark, make_file(tmp_path, 2023, name="b"), schema, year=2023)
    changed = make_file(tmp_path, 2024, rows=[",".join(["9"] * len(HEADER))], name="c")
    assert load(spark, changed, schema) == "loaded"          # new hash replaces 2024 only
    counts = {r["data_year"]: r["count"] for r in spark.table(t).groupBy("data_year").count().collect()}
    assert counts == {2023: 2, 2024: 1}
    assert spark.table(f"{schema}.file_manifest").count() == 2
