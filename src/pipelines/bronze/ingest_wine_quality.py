from pyspark import pipelines as dp

# Same source files used in Databricks' "Build your first ML model" tutorial:
# https://docs.databricks.com/aws/en/getting-started/ml-get-started.html
RAW_PATH_WHITE = "/databricks-datasets/wine-quality/winequality-white.csv"
RAW_PATH_RED = "/databricks-datasets/wine-quality/winequality-red.csv"


@dp.table(
    name="bronze_wine_white",
    comment="Raw white-wine physicochemical data, ingested as-is.",
)
def bronze_wine_white():
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("sep", ";")
        .option("inferSchema", True)
        .load(RAW_PATH_WHITE)
    )


@dp.table(
    name="bronze_wine_red",
    comment="Raw red-wine physicochemical data, ingested as-is.",
)
def bronze_wine_red():
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("sep", ";")
        .option("inferSchema", True)
        .load(RAW_PATH_RED)
    )