def test_ansi_is_on(spark):
    assert spark.conf.get("spark.sql.ansi.enabled") == "true"


def test_delta_roundtrip(spark):
    spark.createDataFrame([(1, "a")], "id INT, v STRING").write.format("delta") \
        .mode("overwrite").saveAsTable("t_roundtrip")
    assert spark.table("t_roundtrip").count() == 1


def _write_year(spark, year, rows):
    df = spark.createDataFrame([(year, r) for r in rows], "data_year INT, v INT")
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", f"data_year = {year}")
       .partitionBy("data_year").saveAsTable("t_replace"))


def test_replace_where_first_write_and_isolation(spark):
    spark.sql("DROP TABLE IF EXISTS t_replace")
    _write_year(spark, 2023, [1, 2])          # first write to a table that doesn't exist
    _write_year(spark, 2024, [10, 20, 30])
    _write_year(spark, 2024, [99])            # rerun replaces 2024 only
    got = {r["data_year"]: r["n"] for r in
           spark.table("t_replace").groupBy("data_year").count().withColumnRenamed("count", "n")
           .collect()}
    assert got == {2023: 2, 2024: 1}
