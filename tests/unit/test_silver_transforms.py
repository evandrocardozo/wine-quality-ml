"""
Unit tests for the transform logic itself, decoupled from the @dp decorators so they
run fast in CI without spinning up a pipeline. The decorated functions in
silver/clean_wine_quality.py call spark.read.table(...) directly, so here we test the
same union + labeling logic against in-memory DataFrames instead of importing the
decorated function.
"""
from pyspark.sql import Row
from pyspark.sql.functions import col, lit, when


def test_union_tags_red_and_white(spark):
    white = spark.createDataFrame([Row(alcohol=9.5, quality=6)])
    red = spark.createDataFrame([Row(alcohol=10.1, quality=7)])

    result = white.withColumn("is_red", lit(0.0)).unionByName(
        red.withColumn("is_red", lit(1.0))
    )

    rows = {r["is_red"]: r["quality"] for r in result.collect()}
    assert rows[0.0] == 6
    assert rows[1.0] == 7


def test_high_quality_label_threshold(spark):
    df = spark.createDataFrame([Row(quality=6), Row(quality=7), Row(quality=9)])
    labeled = df.withColumn(
        "high_quality", when(col("quality") >= 7, 1).otherwise(0)
    )

    labels = [r["high_quality"] for r in labeled.orderBy("quality").collect()]
    assert labels == [0, 1, 1]