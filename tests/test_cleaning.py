from decimal import Decimal

from pyspark.sql import functions as F

from pipeline import cleaning as c


def one(spark, value, expr):
    df = spark.createDataFrame([(value,)], "v STRING")
    return df.select(expr.alias("r")).first()["r"]


def test_safe_cast_gives_null_instead_of_error(spark):
    assert one(spark, " 42 ", c.safe_cast("v", "int")) == 42
    assert one(spark, "17.7", c.safe_cast("v", "decimal(10,2)")) == Decimal("17.70")
    assert one(spark, "abc", c.safe_cast("v", "int")) is None
    assert one(spark, "2024-01-31 23:59:00", c.safe_cast("v", "timestamp")) is not None


def test_bad_type_flags_bad_text_but_not_blank(spark):
    assert one(spark, "abc", c.is_bad_type("v", "int")) is True
    assert one(spark, "", c.is_bad_type("v", "int")) is False
    assert one(spark, None, c.is_bad_type("v", "int")) is False
    assert one(spark, "5", c.is_bad_type("v", "int")) is False


def test_reject_reasons_collects_several_and_empty_for_valid(spark):
    df = spark.createDataFrame([(1,), (20,)], "n INT")
    reasons = c.reject_reasons({"small": F.col("n") < 11, "odd": F.col("n") == 1})
    got = [r["r"] for r in df.select(reasons.alias("r")).orderBy("r").collect()]
    assert [] in got and ["small", "odd"] in got
