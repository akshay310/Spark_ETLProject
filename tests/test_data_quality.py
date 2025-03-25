import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import sys
import os
# Set up the path for module imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from data_quality import validate_and_clean_data

def test_validate_and_clean_data():
    spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
    
    # Sample schema and test data
    schema = StructType([
        StructField("Id", StringType(), True),
        StructField("Title", StringType(), True),
        StructField("review/score", FloatType(), True),
        StructField("Price", FloatType(), True),
        StructField("review/time", StringType(), True),
        StructField("review/helpfulness", StringType(), True)
    ])

    test_data = [
        ("123", "Product A", 4.5, 10.0, "1614556800", "3/5"),  # Valid
        (None, "Product B", 3.0, 5.0, "1614556800", "2/4"),  # Invalid (Id is null)
        ("456", "Product C", None, 8.0, "1614556800", "5/6"),  # Invalid (review/score is null)
        ("789", "Product D", 2.0, -1.0, "1614556800", "1/3"),  # Invalid (Price < 0)
        ("101", "Product E", 5.0, 15.0, "bad_timestamp", "4/7")  # Invalid (review/time incorrect format)
    ]

    df = spark.createDataFrame(test_data, schema)

    config = {
        "etl_config": {
            "data_quality_checks": [
                {"column": "Id", "check": "not_null"},
                {"column": "Title", "check": "not_null"},
                {"column": "review/score", "check": "not_null"},
                {"column": "Price", "check": "greater_than_equal", "value": 0},
                {"column": "review/time", "check": "regex_match", "pattern": r"\\d+"},
                {"column": "review/helpfulness", "check": "regex_match", "pattern": r"\\d+/\\d+"}
            ]
        }
    }

    good_records, bad_records = validate_and_clean_data(df, config)

    assert good_records.count() == 1  # Only one valid row
    assert bad_records.count() == 4  # Four invalid rows

    spark.stop()

if __name__ == "__main__":
    pytest.main()
