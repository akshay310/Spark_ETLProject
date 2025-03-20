import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, MapType
from unittest.mock import patch
from json_read import start_spark, read_json_data
from flatten_json import rename_dataframe_cols, update_column_names, flatten_json_df, clean_column_names
from data_quality_check import get_date_columns, validate_data_quality
from load_to_mysql import write_to_mysql

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
    spark_session = start_spark("/path/to/connector")
    assert isinstance(spark_session, SparkSession)
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
"""
@patch("flatten_json.logging")
def test_read_json_data_file_not_found(mock_logging, spark):
    with pytest.raises(Exception):
        read_json_data(spark, "non_existent.json")
    mock_logging.error.assert_called()
"""
@patch("flatten_json.logging")
def test_rename_dataframe_cols(mock_logging, sample_df):
    renamed_df = rename_dataframe_cols(sample_df, {"name": "full_name"})
    assert "full_name" in renamed_df.columns
    assert "name" not in renamed_df.columns

@patch("flatten_json.logging")
def test_update_column_names(mock_logging, sample_df):
    updated_df = update_column_names(sample_df, 1)
    assert "name*1" in updated_df.columns
    assert "name" not in updated_df.columns

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

@patch("flatten_json.logging")
def test_clean_column_names(mock_logging, sample_df):
    dirty_df = rename_dataframe_cols(sample_df, {"date_of_birth": "date@of#birth!"})
    cleaned_df = clean_column_names(dirty_df)
    assert "date_of_birth" in cleaned_df.columns
    assert "date@of#birth!" not in cleaned_df.columns

@patch("data_quality_check.logging")
def test_get_date_columns(mock_logging, sample_df):
    date_cols = get_date_columns(sample_df)
    assert "date_of_birth" in date_cols

@patch("data_quality_check.logging")
def test_validate_data_quality(mock_logging, spark, sample_df):
    required_columns = ["id", "name"]
    allowed_values = {"name": ["Alice", "Bob"]}
    required_datatypes = {"id": "IntegerType", "name": "StringType"}
    unique_values = ["id"]
    
    good_records, bad_records = validate_data_quality(sample_df, required_columns, allowed_values, required_datatypes, unique_values)
    assert good_records.count() == 2
    assert bad_records.count() == 0
    mock_logging.info.assert_called_with("Data quality validation completed successfully")

@patch("load_to_mysql.logging")
def test_write_to_mysql_failure(mock_logging, sample_df):
    """Test error handling when writing to MySQL fails"""

    with patch("load_to_mysql.DataFrame.write") as mock_write:
        write_to_mysql(sample_df, "jdbc:mysql://localhost:3306/test", "test_table", "user", "password")
        mock_write.format.assert_called_with("jdbc")
        mock_logging.info.assert_called_with("Data successfully written to MySQL")

    with patch("load_to_mysql.DataFrame.write") as mock_write:
        # Simulate an exception when calling `write`
        mock_write.format.side_effect = Exception("Database connection error")

        with pytest.raises(Exception, match="Database connection error"):
            write_to_mysql(sample_df, "jdbc:mysql://localhost:3306/test", "test_table", "user", "password")

        # ✅ Ensure the error is logged
        mock_logging.error.assert_called_with("Error writing data to MySQL: %s", 'Database connection error')