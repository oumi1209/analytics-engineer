"""Test fixtures.

``spark_fixture`` is a **local** SparkSession so the transformation unit tests run
offline (no cluster). ``spark_connect`` is opt-in for integration tests that need a
real Databricks workspace.
"""

import pytest

from analytics_engineer.infra.spark import SparkSessionHandler


@pytest.fixture(scope="session")
def spark_fixture():
    from pyspark.sql import SparkSession

    SparkSessionHandler.set_factory(
        lambda: (
            SparkSession.builder.master("local[*]")
            .appName("analytics-engineer-tests")
            .getOrCreate()
        )
    )
    spark = SparkSessionHandler.get()
    yield spark
    SparkSessionHandler.stop()


@pytest.fixture(scope="session")
def spark_connect():
    """Real Databricks session for integration tests (requires databricks-connect)."""
    from databricks.connect import DatabricksSession

    SparkSessionHandler.set_factory(
        lambda: DatabricksSession.builder.serverless(True).getOrCreate()
    )
    spark = SparkSessionHandler.get()
    yield spark
    SparkSessionHandler.stop()
