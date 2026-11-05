from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from partd.bronze import write_year
from partd.cleaning import (
    is_blank,
    is_invalid_number,
    normalize_name,
    reject_reasons,
    safe_cast,
)
from partd.config import Settings

NAME_COLS = ("Prscrbr_Last_Org_Name", "Prscrbr_First_Name")
# UNVERIFIED until real data is read: the values CMS uses for the geography drug flags.
FLAG_VALUES = ["Y", "N"]
PROVIDER_NUMERIC = {  # silver name: (source column, type)
    "total_claims": ("Tot_Clms", "bigint"),
    "total_30day_fills": ("Tot_30day_Fills", "decimal(18,2)"),
    "total_day_supply": ("Tot_Day_Suply", "bigint"),
    "total_drug_cost": ("Tot_Drug_Cst", "decimal(18,2)"),
    "total_beneficiaries": ("Tot_Benes", "bigint"),
    "ge65_total_claims": ("GE65_Tot_Clms", "bigint"),
    "ge65_total_drug_cost": ("GE65_Tot_Drug_Cst", "decimal(18,2)"),
    "ge65_total_beneficiaries": ("GE65_Tot_Benes", "bigint"),
}
GEO_NUMERIC = {
    "total_prescribers": ("Tot_Prscrbrs", "bigint"),
    "total_claims": ("Tot_Clms", "bigint"),
    "total_30day_fills": ("Tot_30day_Fills", "decimal(18,2)"),
    "total_drug_cost": ("Tot_Drug_Cst", "decimal(18,2)"),
}
GEO_FLAGS = {"is_opioid": "Opioid_Drug_Flag", "is_long_acting_opioid": "Opioid_LA_Drug_Flag",
             "is_antibiotic": "Antbtc_Drug_Flag", "is_antipsychotic": "Antipsych_Drug_Flag"}


def _raw_row(df: DataFrame) -> F.Column:
    """JSON of the Bronze row without names. _corrupt_record holds the whole raw line, names included."""
    keep = [c for c in df.columns if c not in NAME_COLS and c != "_corrupt_record"]
    return F.to_json(F.struct(*keep))


def _split(df: DataFrame, reasons: F.Column, source_table: str, run_id: str):
    df = df.withColumn("_reasons", reasons)
    bad = df.where(F.size("_reasons") > 0)
    quarantine = bad.select(
        "data_year", F.lit(source_table).alias("source_table"),
        F.col("_reasons").alias("reject_reasons"), _raw_row(bad).alias("raw_row"),
        F.lit(run_id).alias("run_id"), F.current_timestamp().alias("quarantined_at"))
    return df.where(F.size("_reasons") == 0), quarantine


def build_provider(bronze: DataFrame, run_id: str):
    """Returns (prescription, prescriber_year, quarantine) for one year of Bronze rows."""
    df = bronze
    for name, (src, typ) in PROVIDER_NUMERIC.items():
        df = df.withColumn(name, safe_cast(src, typ))
    df = (df.withColumn("npi", F.trim("Prscrbr_NPI"))
            .withColumn("brand_name", normalize_name("Brnd_Name"))
            .withColumn("generic_name", normalize_name("Gnrc_Name")))
    rules = {
        "malformed_csv": F.col("_corrupt_record").isNotNull(),
        "invalid_npi": F.coalesce(~F.col("npi").rlike(r"^\d{10}$"), F.lit(True)),
        "missing_drug_name": F.col("brand_name").isNull() | F.col("generic_name").isNull(),
        **{f"non_numeric_{n}": is_invalid_number(src, typ)
           for n, (src, typ) in PROVIDER_NUMERIC.items()},
        "missing_total_claims": is_blank("Tot_Clms"),
        "claims_below_11": F.col("total_claims") < 11,
        "negative_cost": F.col("total_drug_cost") < 0,
        "ge65_claims_exceed_total": F.col("ge65_total_claims") > F.col("total_claims"),
    }
    valid, quarantine = _split(df, reject_reasons(rules), "provider_drug", run_id)
    prescription = valid.select(
        "data_year", "npi", F.col("Prscrbr_State_Abrvtn").alias("state"),
        F.col("Prscrbr_Type").alias("specialty"), "brand_name", "generic_name",
        *PROVIDER_NUMERIC,
        F.col("GE65_Sprsn_Flag").alias("ge65_suppression_flag"),
        F.col("GE65_Bene_Sprsn_Flag").alias("ge65_bene_suppression_flag"),
    ).dropDuplicates()  # drops only rows identical in every column; other duplicates fail the grain check
    prescriber_year = valid.groupBy("data_year", "npi").agg(
        F.max_by("Prscrbr_State_Abrvtn", "total_claims").alias("state"),
        F.max_by("Prscrbr_City", "total_claims").alias("city"),
        F.max_by("Prscrbr_Type", "total_claims").alias("specialty"),
        F.max_by("Prscrbr_Type_Src", "total_claims").alias("specialty_source"))
    return prescription, prescriber_year, quarantine


def build_geo(bronze: DataFrame, run_id: str):
    """Returns (geo_drug, drug_year, quarantine) for one year of Bronze rows."""
    df = bronze
    for name, (src, typ) in GEO_NUMERIC.items():
        df = df.withColumn(name, safe_cast(src, typ))
    df = (df.withColumn("brand_name", normalize_name("Brnd_Name"))
            .withColumn("generic_name", normalize_name("Gnrc_Name")))
    flag_bad = [~F.col(src).isin(FLAG_VALUES) for src in GEO_FLAGS.values()]
    rules = {
        "malformed_csv": F.col("_corrupt_record").isNotNull(),
        "missing_drug_name": F.col("brand_name").isNull() | F.col("generic_name").isNull(),
        **{f"non_numeric_{n}": is_invalid_number(src, typ) for n, (src, typ) in GEO_NUMERIC.items()},
        "negative_cost": F.col("total_drug_cost") < 0,
        "invalid_flag": F.coalesce(F.greatest(*flag_bad), F.lit(False))
                        & (F.col("Prscrbr_Geo_Lvl") == "National"),
    }
    valid, quarantine = _split(df, reject_reasons(rules), "geo_drug", run_id)
    flags = {n: (F.col(src) == "Y") for n, src in GEO_FLAGS.items()}
    geo = valid.select(
        "data_year", F.col("Prscrbr_Geo_Lvl").alias("geo_level"),
        F.col("Prscrbr_Geo_Cd").alias("geo_code"), "brand_name", "generic_name",
        F.col("Prscrbr_Geo_Desc").alias("geo_desc"), *GEO_NUMERIC, *[v.alias(k) for k, v in flags.items()])
    drug_year = (geo.where("geo_level = 'National'")
                 .groupBy("data_year", "brand_name", "generic_name")
                 .agg(*[F.max(k).alias(k) for k in GEO_FLAGS])
                 .withColumn("name_differs_from_generic", F.col("brand_name") != F.col("generic_name")))
    geo = geo.drop(*GEO_FLAGS)
    return geo, drug_year, quarantine


def build_silver(spark: SparkSession, s: Settings, data_year: int, run_id: str) -> None:
    def raw(ds):
        return spark.table(s.table("bronze", f"{ds}_raw")).where(f"data_year = {data_year}")

    p, py, pq = build_provider(raw("provider_drug"), run_id)
    g, dy, gq = build_geo(raw("geo_drug"), run_id)
    for df, name, extra in [(p, "prescription", ""), (py, "prescriber_year", ""),
                            (g, "geo_drug", ""), (dy, "drug_year", ""),
                            (pq, "quarantine", "source_table = 'provider_drug'"),
                            (gq, "quarantine", "source_table = 'geo_drug'")]:
        write_year(df, s.table("silver", name), data_year, extra)
