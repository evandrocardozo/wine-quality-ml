from pyspark import pipelines as dp
from pyspark.sql.functions import lit


@dp.table(
    name="silver_wine_quality",
    comment="Unioned red + white wine data, with data-quality rules enforced.",
)
@dp.expect_or_drop("valid_quality_score", "quality BETWEEN 0 AND 10")
@dp.expect_or_drop("non_null_alcohol", "alcohol IS NOT NULL")
@dp.expect_or_fail("non_negative_volatile_acidity", "`volatile acidity` >= 0")
@dp.expect("plausible_ph", "pH BETWEEN 2.5 AND 4.5")
def silver_wine_quality():
    white = spark.read.table("bronze_wine_white").withColumn("is_red", lit(0.0))
    red = spark.read.table("bronze_wine_red").withColumn("is_red", lit(1.0))
    return white.unionByName(red)