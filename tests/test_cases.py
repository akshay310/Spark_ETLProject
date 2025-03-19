import os
import sys
import json
import pytest
from pyspark.sql import SparkSession
from unittest import mock
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from load_parquet import create_spark_session, read_parquet_data
from load_to_mssql import load_db_config, write_to_mssql
from quality_checks import quality_checks



@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder.master("local[1]").appName("TestETL").getOrCreate()


@pytest.fixture
def sample_parquet(tmp_path, spark):
    """Create a sample parquet dataset for testing."""
    data = [
        (1, 10.5, 15, 50, 3, "Y", 2, 5),
        (2, 0.01, 1, 100, 8, "N", 5, 0),
    ]
    columns = [
        "id", "trip_distance", "fare_amount", "trip_duration", 
        "rate_code", "store_and_fwd_flag", "payment_type","tip_amount"]
    df = spark.createDataFrame(data, columns)

    file_path = str(tmp_path / "test_data.parquet")
    df.write.parquet(file_path)
    return file_path


@pytest.fixture
def db_config(tmp_path):
    """Create a temporary database configuration JSON file."""
    config_path = tmp_path / "config.json"
    config_data = {
        "server": "localhost",
        "database": "test_db",
        "table": "test_table",
        "user": "admin",
        "password": "password",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    with open(config_path, "w") as f:
        json.dump(config_data, f)
    return str(config_path)


def test_create_spark_session():
    """Test if Spark session is created successfully."""
    spark = create_spark_session()
    assert isinstance(spark, SparkSession)
    assert spark.version is not None


def test_read_parquet_data(spark, sample_parquet):
    """Test reading a Parquet file."""
    df = read_parquet_data(spark, sample_parquet)
    assert df is not None
    assert df.count() == 2
    assert set(df.columns) == {
        "id", "trip_distance", "fare_amount", "trip_duration", 
        "rate_code", "store_and_fwd_flag", "payment_type","tip_amount"}


def test_quality_checks(spark, sample_parquet):
    """Test data quality checks."""
    df = read_parquet_data(spark, sample_parquet)
    good_data, bad_data = quality_checks(df)

    assert good_data.count() > 0
    assert bad_data.count() > 0


def test_load_db_config(db_config):
    """Test loading database configuration."""
    config = load_db_config(db_config)
    assert config["database"] == "test_db"
    assert config["user"] == "admin"


def test_write_to_mssql(mocker, spark):
    """Test writing data to MSSQL with a mock DataFrame."""
    mock_df = mocker.Mock()
    db_config = {
        "server": "localhost",
        "database": "test_db",
        "table": "test_table",
        "user": "admin",
        "password": "password",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    mock_df.count.return_value = 5
    mock_write = mocker.patch.object(mock_df, "write")

    write_to_mssql(mock_df, db_config)

    mock_write.format.assert_called_once_with("jdbc")