from pyspark import pipelines as dp
from pyspark.sql import DataFrame

# Same source files used in Databricks' "Build your first ML model" tutorial:
# https://docs.databricks.com/aws/en/getting-started/ml-get-started.html
RAW_PATH_WHITE = "/databricks-datasets/wine-quality/winequality-white.csv"
RAW_PATH_RED = "/databricks-datasets/wine-quality/winequality-red.csv"


def _read_wine_csv(path: str) -> DataFrame:
    """Load one semicolon-delimited wine CSV with its headers snake_cased.

    The source headers contain spaces ("fixed acidity", "free sulfur dioxide").
    Delta rejects ' ,;{}()\\n\\t=' in column names outright
    (DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES), so the data cannot land at all
    without either renaming or turning on column mapping. Renaming is the smaller
    change: it keeps the Delta protocol at its default reader/writer versions and
    keeps downstream expectations readable without backticks.
    """
    df = (
        spark.read.format("csv")
        .option("header", True)
        .option("sep", ";")
        .option("inferSchema", True)
        .load(path)
    )
    return df.toDF(*[c.strip().lower().replace(" ", "_") for c in df.columns])


@dp.table(
    name="bronze_wine_white",
    comment="Raw white-wine physicochemical data; headers snake_cased, values as-is.",
)
def bronze_wine_white():
    return _read_wine_csv(RAW_PATH_WHITE)


@dp.table(
    name="bronze_wine_red",
    comment="Raw red-wine physicochemical data; headers snake_cased, values as-is.",
)
def bronze_wine_red():
    return _read_wine_csv(RAW_PATH_RED)
