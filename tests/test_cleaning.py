from decimal import Decimal

from pyspark.sql import functions as F

from partd import cleaning as c


def one(spark, value, expr):
    df = spark.createDataFrame([(value,)], "v STRING")
    return df.select(expr.alias("r")).first()["r"]


def test_normalize_name(spark):
    assert one(spark, "  lipitor ", c.normalize_name("v")) == "LIPITOR"
    assert one(spark, " a   b  c ", c.normalize_name("v")) == "A B C"
    assert one(spark, "   ", c.normalize_name("v")) is None
    assert one(spark, "", c.normalize_name("v")) is None


def test_blank_stays_null_and_bad_text_is_flagged(spark):
    assert one(spark, "", c.safe_cast("v", "decimal(18,2)")) is None
    assert one(spark, "", c.is_invalid_number("v", "bigint")) is False
    for bad in ("abc", "12.5", "1,200"):
        assert one(spark, bad, c.is_invalid_number("v", "bigint")) is True, bad
    assert one(spark, " 42 ", c.safe_cast("v", "bigint")) == 42


def test_reject_reasons_collects_several_and_empty_for_valid(spark):
    df = spark.createDataFrame([(1,), (20,)], "n INT")
    reasons = c.reject_reasons({"small": F.col("n") < 11, "odd": F.col("n") == 1})
    got = [r["r"] for r in df.select(reasons.alias("r")).orderBy("r").collect()]
    assert [] in got and ["small", "odd"] in got


def test_decimal_sum_is_exact(spark):
    df = spark.createDataFrame([("0.10",), ("0.20",)] * 5, "v STRING")
    total = df.select(F.sum(c.safe_cast("v", "decimal(18,2)")).alias("t")).first()["t"]
    assert total == Decimal("1.50")
