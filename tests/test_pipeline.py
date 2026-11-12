"""Whole job on a tiny month, run twice: the second run must leave every table the same."""
from types import SimpleNamespace

import pytest

from helpers import COLS, CONFIG, SQL, trip
from pipeline import tasks
from pipeline.quality import QualityCheckError
from pipeline.tasks import latest_month

ORDER = ["load_bronze", "check_bronze", "build_silver", "check_silver", "build_gold", "check_gold"]


def land(spark, root, rows):
    spark.createDataFrame([tuple(r[c] for c in COLS) for r in rows],
                          ", ".join(f"{c} STRING" for c in COLS)) \
        .write.mode("overwrite").parquet(str(root / "trips" / "month=2024-01"))
    (root / "zones").mkdir(exist_ok=True)
    (root / "zones" / "zones.csv").write_text(
        'LocationID,Borough,Zone,service_zone\n1,EWR,"Newark Airport",EWR\n2,Queens,Jamaica Bay,Boro Zone\n')


@pytest.fixture
def args(spark, tmp_path):
    for layer in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"DROP DATABASE IF EXISTS e2e_{layer} CASCADE")
    return SimpleNamespace(month="2024-01", run_id="e2e", catalog="spark_catalog",
                           schema_prefix="e2e", config_dir=CONFIG, sql_dir=SQL,
                           landing_root=str(tmp_path))


def run_job(spark, a):
    for name in ORDER:
        tasks.TASKS[name](spark, a)
    return {t: spark.table(f"e2e_{t}").count() for t in
            ("bronze.trips_raw", "silver.trips", "silver.quarantine", "gold.daily_summary")}


def test_full_job_and_rerun_is_identical(spark, args, tmp_path):
    # 40 good trips (distinct times) + 1 duplicate = 41 rows, 1 quarantined: 2.4%, under the 5% limit
    good = [trip(pickup=f"2024-01-{d:02d} 08:{m:02d}:00", dropoff=f"2024-01-{d:02d} 09:{m:02d}:00")
            for d in (1, 2) for m in range(20)]
    land(spark, tmp_path, good + [good[0]])
    first = run_job(spark, args)
    assert first == {"bronze.trips_raw": 41, "silver.trips": 40, "silver.quarantine": 1,
                     "gold.daily_summary": 2}
    assert run_job(spark, args) == first
    statuses = {r["status"] for r in spark.table("e2e_ops.dq_results").collect()}
    assert statuses == {"pass"}


def test_latest_month_picks_the_newest_landed_folder(tmp_path):
    for m in ("2024-01", "2024-03", "2024-02"):
        (tmp_path / "trips" / f"month={m}").mkdir(parents=True)
    (tmp_path / "trips" / "_tmp").mkdir()                              # not a month folder
    assert latest_month(str(tmp_path)) == "2024-03"


def test_latest_month_with_nothing_landed_fails(tmp_path):
    (tmp_path / "trips").mkdir()
    with pytest.raises(ValueError, match="no month folders"):
        latest_month(str(tmp_path))


def test_bad_month_stops_at_silver_check_and_gold_is_not_built(spark, args, tmp_path):
    land(spark, tmp_path, [trip(), trip(distance="0")])               # 50% invalid
    with pytest.raises(QualityCheckError, match="invalid_row_rate"):
        run_job(spark, args)
    assert not spark.catalog.tableExists("e2e_gold.daily_summary")
