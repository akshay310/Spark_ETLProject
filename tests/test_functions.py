import pytest
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from unittest.mock import patch, MagicMock, mock_open
import json
from json_read import read_json_data, start_spark
from flatten_json import flatten_json_df, clean_column_names, rename_dataframe_cols, update_column_names
from data_quality_check import validate_data_quality
from load_to_mysql import write_to_mysql
from read_config import load_json_req, get_input_file, get_bad_file, get_checks
from write_bad_records import write_parquet

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

@pytest.fixture
def sample_df(spark):
    return spark.createDataFrame([
        (1, "Alice", "2025-03-19"),
        (2, "Bob", "2024-12-25"),
    ], ["id", "name", "date_of_birth"])

@patch("json_read.logging")
def test_start_spark(mock_logging):
    with patch("json_read.SparkSession.builder.getOrCreate") as mock_spark:
        mock_spark.return_value = MagicMock()
        spark = start_spark()
        assert spark is not None
        mock_logging.info.assert_called_with("Spark session started successfully.")

@patch("json_read.logging")
def test_read_json_data(mock_logging, spark, tmp_path):
    json_file = tmp_path / "test.json"
    json_file.write_text('[{"name": "Alice"}, {"name": "Bob"}]')
    df = read_json_data(spark, str(json_file))
    assert df.count() == 2
    with pytest.raises(Exception):
        read_json_data(spark, "non_existent.json")
    mock_logging.error.assert_called()
    mock_logging.info.assert_called_with("Successfully read JSON file: %s", str(json_file))

def test_rename_dataframe_cols(spark):
    df = spark.createDataFrame([(1, "test")], ["id", "name"])
    col_mapping = {"id": "user_id", "name": "full_name"}
    renamed_df = rename_dataframe_cols(df, col_mapping)
    assert "user_id" in renamed_df.columns
    assert "full_name" in renamed_df.columns

def test_update_column_names():
    spark = start_spark()
    df = spark.createDataFrame([(1, "test")], ["id", "name"])
    updated_df = update_column_names(df, 2)
    assert "id*2" in updated_df.columns
    assert "name*2" in updated_df.columns

@patch("flatten_json.logging")
def test_flatten_json_df(mock_logging, spark):
    """Test flattening a nested JSON structure in a Spark DataFrame"""
    
    # Define schema with a nested StructType
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("details", StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ]), True)
    ])
    
    # Sample data
    data = [(1, ("Alice", 25))]
    df = spark.createDataFrame(data, schema)

    # Call function to flatten JSON
    flattened_df = flatten_json_df(df)

    # ✅ Assertions
    assert "details*1->name*2" in flattened_df.columns
    assert "details*1->age*2" in flattened_df.columns

    # ✅ Check logging messages
    mock_logging.info.assert_any_call("Flattening JSON DataFrame......")
    mock_logging.info.assert_any_call("Flattening complete.")

def test_clean_column_names():
    spark = start_spark()
    df = spark.createDataFrame([(1, "test")], ["id@!", "name#*"])
    cleaned_df = clean_column_names(df)
    assert "id" in cleaned_df.columns
    assert "name" in cleaned_df.columns

def test_validate_data_quality():
    spark = start_spark()
    df = spark.createDataFrame([(1, "valid")], ["id", "status"])
    checks = {
        "column_values_to_not_be_null": ["id"],
        "column_values_to_be_in_set": {"status": ["valid", "invalid"]},
        "column_values_to_be_of_type": {"id": "IntegerType"},
        "column_values_to_be_unique": ["id"]
    }
    good_records, bad_records = validate_data_quality(df, checks)
    assert good_records.count() == 1
    assert bad_records.count() == 0

def test_write_parquet(spark,tmp_path):
    
    # Create a sample DataFrame
    data = [(1, "error1"), (2, "error2")]
    schema = ["id", "error_message"]
    df = spark.createDataFrame(data, schema=schema)
    
    # Define temporary file path
    file_path = str(tmp_path / "bad_records")
    
    # Call the function
    write_parquet(df, file_path)
    
    # Check if the Parquet file was created
    assert os.path.exists(file_path), "Parquet file was not created"
    
    # Read back the Parquet file to verify contents
    df_read = spark.read.format("parquet").load(file_path)
    
    assert df_read.count() == df.count(), "Row count mismatch"
    assert df_read.columns == df.columns, "Column mismatch"

@patch("load_to_mysql.logging")
def test_write_to_mysql_failure(mock_logging, sample_df):
    """Test error handling when writing to MySQL fails"""

    with patch("load_to_mysql.DataFrame.write") as mock_write:
        write_to_mysql(sample_df, "test_table")
        mock_write.format.assert_called_with("jdbc")
        mock_logging.info.assert_called_with("Data successfully written to MySQL")

    with patch("load_to_mysql.DataFrame.write") as mock_write:
        # Simulate an exception when calling `write`
        mock_write.format.side_effect = Exception("Database connection error")

        with pytest.raises(Exception, match="Database connection error"):
            write_to_mysql(sample_df,"test_table")

        # ✅ Ensure the error is logged
        mock_logging.error.assert_called_with("Error writing data to MySQL: %s", 'Database connection error')

@pytest.fixture
def mock_json_data():
    return {
        "task": {
            "source": {
                "file_path": "/data/source/",
                "file_name": "input.csv"
            },
            "target": {
                "bad_record_file_path": "/data/errors/",
                "bad_record_file_name": "bad_records.csv"
            },
            "data_quality": [
                {"check": "check_unique", "parameters": {"column": "id"}},
                {"check": "check_null", "parameters": {"column": "name"}},
                {"check": "check_type", "parameters": {"column": "age", "type": "int"}},
                {"check": "check_set", "parameters": {"column": "status", "value_set": "active, inactive, pending"}}
            ]
        }
    }

@patch("builtins.open", new_callable=mock_open, read_data='{}')
@patch("json.load", side_effect=json.JSONDecodeError("Invalid JSON", "", 0))
@patch("logging.error")
def test_load_json_req_invalid_json(mock_log, mock_json, mock_file):
    with pytest.raises(json.JSONDecodeError):
        load_json_req("data.json")
    mock_log.assert_called_with("Error decoding JSON file: %s", "data.json")

@patch("builtins.open", side_effect=FileNotFoundError)
@patch("logging.error")
def test_load_json_req_file_not_found(mock_log, mock_file):
    with pytest.raises(FileNotFoundError):
        load_json_req("missing.json")
    mock_log.assert_called_with("JSON file not found: %s", "missing.json")

@patch("builtins.open", new_callable=mock_open)
@patch("json.load")
def test_load_json_req_success(mock_json, mock_file, mock_json_data):
    mock_json.return_value = mock_json_data
    data = load_json_req("data.json")
    assert data == mock_json_data

@patch("read_config.load_json_req")
@patch("logging.error")
def test_get_input_file_missing_key(mock_log, mock_load_json):
    mock_load_json.return_value = {"task": {}}
    with pytest.raises(KeyError):
        get_input_file("data.json")
    mock_log.assert_called()

@patch("read_config.load_json_req")
def test_get_input_file_success(mock_load_json, mock_json_data):
    mock_load_json.return_value = mock_json_data
    result = get_input_file("data.json")
    assert result == "/data/source/input.csv"

@patch("read_config.load_json_req")
@patch("logging.error")
def test_get_bad_file_missing_key(mock_log, mock_load_json):
    mock_load_json.return_value = {"task": {}}
    with pytest.raises(KeyError):
        get_bad_file("data.json")
    mock_log.assert_called()

@patch("read_config.load_json_req")
def test_get_bad_file_success(mock_load_json, mock_json_data):
    mock_load_json.return_value = mock_json_data
    result = get_bad_file("data.json")
    assert result == "/data/errors/bad_records.csv"

@patch("read_config.load_json_req")
@patch("logging.error")
def test_get_checks_missing_key(mock_log, mock_load_json):
    mock_load_json.return_value = {"task": {}}
    with pytest.raises(KeyError):
        get_checks("data.json")
    mock_log.assert_called()

@patch("read_config.load_json_req")
def test_get_checks_success(mock_load_json, mock_json_data):
    mock_load_json.return_value = mock_json_data
    result = get_checks("data.json")
    expected = {
        "check_unique": ["id"],
        "check_null": ["name"],
        "check_type": {"age": "int"},
        "check_set": {"status": ["active", "inactive", "pending"]}
    }
    assert result == expected