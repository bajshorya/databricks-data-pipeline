from pathlib import Path

from partd.config import load_settings, load_sources

CONFIG = Path(__file__).parent.parent / "config"


def test_settings_load():
    s = load_settings(CONFIG, catalog="spark_catalog")
    assert s.table("silver", "prescription") == "spark_catalog.partd_silver.prescription"
    assert s.landing_root == "/Volumes/spark_catalog/partd_ops/landing"
    assert s.years == (2022, 2023)


def test_sources_have_both_datasets_and_years():
    src = load_sources(CONFIG)
    for dataset in ("provider_drug", "geo_drug"):
        assert set(src[dataset]) == {"2023", "2024"}
