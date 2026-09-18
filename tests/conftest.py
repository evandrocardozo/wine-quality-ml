"""Shared fixtures for the unit tests.

These tests deliberately run against a local SparkSession rather than a Databricks
cluster, so CI can verify the transform logic on a PR without a workspace or any
credentials. The session is module-wide because JVM startup dominates the runtime of
the tests themselves.
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("wine-quality-unit-tests")
        # Defaults are tuned for a cluster; on a single local core they just add
        # shuffle overhead and bind a UI port CI doesn't need.
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
