from unittest.mock import patch, MagicMock, PropertyMock
import pytest
from pyspark.sql import SparkSession
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from load_to_oracle import save_to_oracle

# Hardcoded Spark session
@pytest.fixture(scope="session")
def spark():
    """Creates a Spark session for testing."""
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("test_spark_session") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def mock_spark_write():
    with patch("pyspark.sql.DataFrame.write", new_callable=PropertyMock) as mock_write:
        mock_writer = MagicMock()
        mock_write.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.save.return_value = None  # Simulate successful save
        yield mock_writer

@pytest.fixture
def sample_df(spark):
    """Creates a small sample DataFrame."""
    data = [
        (1, "Alice", 25),
        (2, "Bob", 30),
        (3, "Charlie", 35)
    ]
    columns = ["id", "name", "age"]
    return spark.createDataFrame(data, columns)

def test_save_to_oracle(spark, sample_df, monkeypatch, mock_spark_write):
    """Test save_to_oracle function with a mock write operation."""
    monkeypatch.setenv("ORACLE_URL", "jdbc:oracle:thin:@localhost:1521/XEPDB1")
    monkeypatch.setenv("ORACLE_USER", "system")
    monkeypatch.setenv("ORACLE_PASSWORD", "testpassword")
    monkeypatch.setenv("ORACLE_DRIVER", "oracle.jdbc.OracleDriver")

    save_to_oracle(sample_df, "TEST_TABLE")

    # Ensure the save() function is called
    mock_spark_write.save.assert_called_once()
