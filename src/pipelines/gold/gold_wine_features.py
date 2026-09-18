from pyspark import pipelines as dp
from pyspark.sql.functions import col, when


@dp.materialized_view(
    name="gold_wine_features",
    comment=(
        "ML-ready feature table: physicochemical features + binary high_quality label."
    ),
)
def gold_wine_features():
    df = spark.read.table("silver_wine_quality")
    return df.withColumn("high_quality", when(col("quality") >= 7, 1).otherwise(0))