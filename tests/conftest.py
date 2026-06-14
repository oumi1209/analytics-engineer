import pytest
from databricks.connect import DatabricksSession

@pytest.fixture(scope="session")
def spark_fixture():
    return DatabricksSession.builder.serverless(True).getOrCreate()