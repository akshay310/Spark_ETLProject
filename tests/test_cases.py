import os
import sys
import json
import pytest
import logging
from pyspark.sql import SparkSession
from unittest.mock import MagicMock, patch, mock_open

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config_read import load_config
from parquet_read import create_spark_session, read_parquet_data
from mssql_write import write_to_mssql
from quality_checks import quality_checks, apply_check

DB_CONFIG = {
    "database": {
        "db_name": "TestDB",
        "db_table": "TestTable"
    }
}

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def spark():
    """Fixture to create a Spark session for testing."""
    return create_spark_session()

@pytest.fixture
def sample_df(spark):
    """Fixture to create a sample PySpark DataFrame for testing."""
    return spark.createDataFrame([(1, "test")], ["id", "name"])

def test_create_spark_session(spark):
    """Test that a Spark session is created successfully."""
    assert isinstance(spark, SparkSession)
    assert spark.version is not None  # Ensure Spark session is active

@patch("pyspark.sql.SparkSession.read")
def test_read_parquet_data(mock_spark_read, spark):
    """Test reading parquet data with mocked Spark read."""
    mock_df = MagicMock()
    mock_spark_read.parquet.return_value = mock_df  # Mock parquet file reading
    mock_df.count.return_value = 5  # Mock row count
    mock_df.select.return_value = mock_df  # Mock column selection

    config = {
        "source": {
            "file_path": "/path/to/data",
            "file_name": "test.parquet",
            "select_columns": "col1,col2"
        }
    }

    df = read_parquet_data(spark, config)

    file_path = os.path.join(config["source"]["file_path"], config["source"]["file_name"])
    mock_spark_read.parquet.assert_called_once_with(file_path)
    mock_df.select.assert_called_once_with("col1", "col2")
    assert df.count() == 5  # Ensure row count matches expected value

    logger.info("read_parquet_data test passed.")


def test_read_parquet_data_exception():
    """Test read_parquet_data function when an exception occurs during file reading."""
    
    mock_spark = MagicMock()  # Mock SparkSession
    mock_config = {
        "source": {
            "file_path": "/invalid/path",
            "file_name": "data.parquet",
            "select_columns": ""
        }
    }

    expected_file_path = os.path.join(mock_config["source"]["file_path"], mock_config["source"]["file_name"])

    with patch.object(mock_spark.read, "parquet", side_effect=Exception("File not found")), \
         patch("parquet_read.logger") as mock_logger:

        # Ensure the function raises an exception when file reading fails
        with pytest.raises(Exception, match="File not found"):
            read_parquet_data(mock_spark, mock_config)

        # Verify that the logger recorded the error with the exact file path
        mock_logger.exception.assert_called_with(
            "Failed to read parquet data from '%s': %s", expected_file_path, "File not found"
        )
        

def test_apply_check():
    """Test apply_check function with different rule types."""
    mock_ge_df = MagicMock()
    mock_df = MagicMock()
    
    rule_unique = {"check": "column_values_to_be_unique", "parameters": {"column": "id"}}
    mock_ge_df.expect_column_values_to_be_unique.return_value = {"success": True}
    success, failed = apply_check(mock_ge_df, mock_df, rule_unique)
    assert success is True
    
    rule_null = {"check": "column_values_to_not_be_null", "parameters": {"column": "fare_amount"}}
    mock_ge_df.expect_column_values_to_not_be_null.return_value = {"success": False, "result": {"partial_unexpected_list": [None]}}
    success, failed = apply_check(mock_ge_df, mock_df, rule_null)
    assert success is False
    assert failed == [None]

    rule_in_set = {"check": "column_values_to_be_in_set", "parameters": {"column": "payment_type", "value_set": ["cash", "credit"]}}
    mock_ge_df.expect_column_values_to_be_in_set.return_value = {"success": False, "result": {"partial_unexpected_list": ["debit", "voucher"]}}
    success, failed = apply_check(mock_ge_df, mock_df, rule_in_set)
    assert success is False
    assert failed == ["debit", "voucher"]

    rule_dynamic_between = {"check": "column_values_to_be_between", "parameters": {"column": "fare_amount", "min_value": 10, "max_value": "fare_amount * 0.5"}}
    mock_df.select.return_value.first.return_value = [100]  # Suppose `fare_amount` first value is 100
    expected_max_value = 100 * 0.5  # Should be 50
    mock_ge_df.expect_column_values_to_be_between.return_value = {"success": False, "result": {"partial_unexpected_list": [60]}}

    success, failed = apply_check(mock_ge_df, mock_df, rule_dynamic_between)
    assert success is False
    assert failed == [60]

def test_apply_check_unsupported_check(caplog):
    """Test apply_check function with an unsupported check type."""
    mock_ge_df = MagicMock()
    mock_df = MagicMock()

    # Define an unsupported rule
    rule_unsupported = {"check": "columns_to_be_null", "parameters": {"column": "fare_amount"}}
    # Expect an exception or failure response
    with caplog.at_level(logging.WARNING):
        success, failed = apply_check(mock_ge_df, mock_df, rule_unsupported)

    # Assert the function returned expected values
    assert success is True 
    assert failed == []  

    # Assert warning message was logged
    assert "Unsupported check type: columns_to_be_null" in caplog.text

    
def test_apply_check_exception_handling(caplog):
    """Test apply_check function when an exception occurs."""
    mock_ge_df = MagicMock()
    mock_df = MagicMock()

    rule_exception = {"check": "column_values_to_be_unique", "parameters": {"column": "id"}}
    mock_ge_df.expect_column_values_to_be_unique.side_effect = Exception("Test exception")

    # Capture logs
    with caplog.at_level(logging.ERROR):
        success, failed = apply_check(mock_ge_df, mock_df, rule_exception)

    # Assertions
    assert success is False 
    assert failed == []

    # Ensure the correct error message is logged
    assert "Error applying check 'column_values_to_be_unique' on column 'id': Test exception" in caplog.text

def test_quality_checks(spark, tmp_path):
    """Test quality_checks function."""
    
    # Sample test data
    test_data = [(1, 10.5, 15, 50, 3, "Y", 2, 5), (2, 0.01, 1, 100, 8, "N", 5, 0)]
    columns = ["id", "trip_distance", "fare_amount", "trip_duration", "rate_code", "store_and_fwd_flag", "payment_type", "tip_amount"]
    df = spark.createDataFrame(test_data, columns)

    # Use a temporary directory for storing bad/good records
    bad_records_path = tmp_path / "bad_records"
    good_records_path = tmp_path / "good_records"
    os.makedirs(bad_records_path, exist_ok=True)
    os.makedirs(good_records_path, exist_ok=True)

    config = {
        "data_quality": [
            {"check": "column_values_to_be_between", "parameters": {"column": "fare_amount", "min_value": 1, "max_value": 10}}
        ],
        "target": {
            "bad_record_file_path": str(bad_records_path),
            "bad_record_file_name": "bad_data.parquet",
            "good_record_file_path": str(good_records_path),
            "good_record_file_name": "good_data.parquet"
        }
    }

    # Run the quality_checks function
    good_data, bad_data = quality_checks(df, config)

    # Ensure records are classified correctly
    assert good_data.count() == 1, "Expected 1 good record, but got a different count"
    assert bad_data.count() == 1, "Expected 1 bad record, but got a different count"

    # Ensure the files were written
    assert bad_records_path.exists(), "Bad records directory was not created"
    assert good_records_path.exists(), "Good records directory was not created"

def test_quality_checks_exception():
    """Test quality_checks function when an exception occurs."""
    mock_df = MagicMock()
    mock_config = {
        "data_quality": [
            {"check": "column_values_to_be_unique", "parameters": {"column": "id"}}
        ],
        "target": {
            "bad_record_file_path": "/path/to/bad",
            "bad_record_file_name": "bad.parquet",
            "good_record_file_path": "/path/to/good",
            "good_record_file_name": "good.parquet"
        }
    }

    # Simulating an exception when applying checks
    with patch("quality_checks.apply_check", side_effect=Exception("Test exception")), \
         patch("quality_checks.logger") as mock_logger:
        
        with pytest.raises(Exception, match="Test exception"):
            quality_checks(mock_df, mock_config)

        # Check if the exception was logged
        mock_logger.exception.assert_called_with("Error during data quality checks: %s", "Test exception")

@patch("pyspark.sql.DataFrame.write")
def test_write_to_mssql_success(mock_write, sample_df):
    """Test if write_to_mssql writes data successfully."""
    mock_jdbc = MagicMock()
    mock_write.format.return_value = mock_jdbc
    mock_jdbc.option.return_value = mock_jdbc
    mock_jdbc.mode.return_value = mock_jdbc
    write_to_mssql(sample_df, DB_CONFIG)
   
    mock_write.format.assert_called_once_with("jdbc")
    mock_jdbc.option.assert_any_call("dbtable", "dbo.TestTable")
    mock_jdbc.option.assert_any_call("user", os.getenv("DB_USER"))
    mock_jdbc.save.assert_called_once()

# Test database connection failure
@patch("pyspark.sql.DataFrame.write")
def test_write_to_mssql_failure(mock_write, sample_df):
    """Test if write_to_mssql handles failure correctly."""
    mock_jdbc = MagicMock()
    mock_write.format.return_value = mock_jdbc
    mock_jdbc.option.return_value = mock_jdbc
    mock_jdbc.mode.return_value = mock_jdbc
    mock_jdbc.save.side_effect = Exception("Database connection error")

    with pytest.raises(Exception, match="Database connection error"):
        write_to_mssql(sample_df, DB_CONFIG)

# Test loading a valid JSON config file
@patch("builtins.open", new_callable=mock_open, read_data='{"database": {"db_name": "testDB", "db_table": "testTable"}}')
def test_load_config_valid(mock_file):
    """Test if load_config loads a valid JSON configuration file correctly."""
    config = load_config("config.json")
    assert config["database"]["db_name"] == "testDB"
    assert config["database"]["db_table"] == "testTable"

# Test handling a missing config file
@patch("builtins.open", side_effect=FileNotFoundError("File not found"))
def test_load_config_missing(mock_file):
    """Test if load_config raises an error when the file is missing."""
    with pytest.raises(FileNotFoundError, match="File not found"):
        load_config("missing_config.json")

# Test handling an invalid JSON file
@patch("builtins.open", new_callable=mock_open, read_data="{invalid_json}")
def test_load_config_invalid_json(mock_file):
    """Test if load_config raises an error for invalid JSON format."""
    with patch("json.load", side_effect=json.JSONDecodeError("Invalid JSON", "", 0)):
        with pytest.raises(json.JSONDecodeError):
            load_config("invalid_config.json")

# Test handling a permission error when accessing the file
@patch("builtins.open", side_effect=PermissionError("Permission denied"))
def test_load_config_permission_error(mock_file):
    """Test if load_config raises an error when file access is denied."""
    with pytest.raises(PermissionError, match="Permission denied"):
        load_config("config.json")
