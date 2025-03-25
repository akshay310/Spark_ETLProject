import pytest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock, patch
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import main

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    yield spark
    spark.stop()

@patch("main.load_data")
@patch("main.validate_and_clean_data")
@patch("main.save_data")
@patch("main.create_table_if_not_exists")
@patch("main.save_to_oracle")
@patch("logging.info")
def test_main(mock_log, mock_save_to_oracle, mock_create_table, mock_save_data, mock_validate, mock_load, spark):
    """Tests the main ETL function end-to-end with mocks."""

    # Mock return values
    mock_df = MagicMock()
    mock_good_df = MagicMock()
    mock_bad_df = MagicMock()

    mock_load.return_value = mock_df
    mock_validate.return_value = (mock_good_df, mock_bad_df)

    # Run the function
    config = {
        "etl_config": {
            "source": {"file_path": "test_data.csv"},
            "target": {"bad_records_path": "/tmp/bad_records.parquet", "table_name": "test_table"},
        }
    }
    
    main.main(config)
