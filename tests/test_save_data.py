import pytest
import logging
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from save_data import save_data  # Import your function

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
def sample_dfs(spark):
    """Creates sample good and bad records DataFrames."""
    good_data = [(1, "Alice"), (2, "Bob")]
    bad_data = [(3, "Charlie"), (4, "David")]

    good_records_df = spark.createDataFrame(good_data, ["id", "name"])
    bad_records_df = spark.createDataFrame(bad_data, ["id", "name"])

    return good_records_df, bad_records_df

@patch("pyspark.sql.DataFrame.write")  # Mock the write function
@patch("logging.info")  # Mock logging
def test_save_data(mock_log, mock_write, sample_dfs):
    """Tests the save_data function."""
    good_records_df, bad_records_df = sample_dfs
    bad_records_path = "/tmp/bad_records.parquet"

    save_data(good_records_df, bad_records_df, bad_records_path)

    # ✅ Bad records should be written to Parquet
    mock_write.mode.return_value.parquet.assert_called_once_with(bad_records_path)

    # ✅ Logs should be correctly called
    mock_log.assert_any_call("Saved %d bad records to %s.", 2, bad_records_path)
    mock_log.assert_any_call("%d good records ready for Oracle ingestion.", 2)
