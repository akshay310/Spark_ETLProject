import pytest
import logging
from unittest.mock import MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from json_read import read_json_data
from flatten_json import rename_dataframe_cols, update_column_names, flatten_json_df, clean_column_names
from data_quality_check import get_date_columns, validate_data_quality
from load_to_mysql import write_to_mysql

@pytest.fixture(scope="session")

def spark():
    """Fixture to create a Spark session for testing"""
    return SparkSession.builder.appName("TestApp").getOrCreate()

def test_read_json_data(spark, tmp_path):
    """Test reading JSON data into a Spark DataFrame"""
    test_json = tmp_path / "test.json"
    test_json.write_text('{"name": "Alice", "age": 25}\n{"name": "Bob", "age": 30}')

    df = read_json_data(spark, str(test_json))
    assert df is not None
    assert df.count() == 2
    assert "name" in df.columns
    assert "age" in df.columns

def test_rename_dataframe_cols(spark):
    """Test renaming columns in a DataFrame"""
    data = [("Alice", 25), ("Bob", 30)]
    schema = StructType([StructField("name", StringType(), True), StructField("age", IntegerType(), True)])
    df = spark.createDataFrame(data, schema)

    renamed_df = rename_dataframe_cols(df, {"name": "full_name", "age": "years_old"})
    assert "full_name" in renamed_df.columns
    assert "years_old" in renamed_df.columns

def test_update_column_names(spark):
    """Test updating column names with index"""
    data = [("Alice", 25)]
    df = spark.createDataFrame(data, ["name", "age"])
    updated_df = update_column_names(df, 1)
    
    assert "name*1" in updated_df.columns
    assert "age*1" in updated_df.columns

def test_flatten_json_df(spark):
    """Test flattening a nested JSON structure in a Spark DataFrame"""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("details", StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ]), True)
    ])
    data = [(1, ("Alice", 25))]
    df = spark.createDataFrame(data, schema)

    flattened_df = flatten_json_df(df)
    assert "details*1->name*2" in flattened_df.columns
    assert "details*1->age*2" in flattened_df.columns

def test_clean_column_names(spark):
    """Test cleaning column names"""
    data = [("Alice", 25)]
    df = spark.createDataFrame(data, ["Na!me", "A ge"])
    
    cleaned_df = clean_column_names(df)
    assert "Na_me" in cleaned_df.columns
    assert "A_ge" in cleaned_df.columns

def test_get_date_columns(spark):
    """Test extracting date columns"""
    data = [("2024-01-01", "Alice"), ("2024-01-02", "Bob")]
    df = spark.createDataFrame(data, ["event_date", "name"])
    
    date_cols = get_date_columns(df)
    assert "event_date" in date_cols
    assert "name" not in date_cols

def test_validate_data_quality(spark):
    """Test data quality validation"""
    data = [("Alice", "2024-01-01", "New York"), ("Bob", "Invalid Date", "LA")]
    df = spark.createDataFrame(data, ["name", "event_date", "city"])

    required_columns = ["name"]
    allowed_values = {"city": ["New York", "LA", "SF"]}
    required_datatypes = {"name": "StringType"}
    unique_values = ["name"]

    good_records, bad_records = validate_data_quality(df, required_columns, allowed_values, required_datatypes, unique_values)

    assert good_records.count() > 0
    assert bad_records.count() >= 0

def test_write_to_mysql(mocker, spark):
    """Test writing data to MySQL with a mock DataFrame."""
    
    # Mock DataFrame
    mock_df = mocker.Mock(spec=DataFrame)

    # Mock `write` property and method chaining
    mock_write = MagicMock()
    mock_df.write = mock_write
    mock_write.format.return_value = mock_write
    mock_write.option.return_value = mock_write
    mock_write.save.return_value = None  # Simulate successful save

    # Mock logging
    mock_logger = mocker.patch.object(logging, "info")
    mocker.patch.object(logging, "error")

    # MySQL Config
    db_config = {
        "url": "jdbc:mysql://localhost/test_db",
        "dbtable": "test_table",
        "user": "root",
        "password": "",
    }

    # Call the function
    write_to_mysql(mock_df, **db_config)

    # ✅ Assertions
    mock_write.format.assert_called_once_with("jdbc")
    mock_write.option.assert_any_call("driver", "com.mysql.cj.jdbc.Driver")
    mock_write.option.assert_any_call("url", db_config["url"])
    mock_write.option.assert_any_call("dbtable", db_config["dbtable"])
    mock_write.option.assert_any_call("user", db_config["user"])
    mock_write.option.assert_any_call("password", db_config["password"])
    mock_write.save.assert_called_once()
    
    # ✅ Ensure logging messages were called
    mock_logger.assert_any_call(f"Starting data write to MySQL table: {db_config['dbtable']}")
    mock_logger.assert_any_call("Data successfully written to MySQL")
