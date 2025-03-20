"""
Unit tests for PySpark ETL pipeline using pytest.
"""

import pytest
import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit
from load_data import load_data, detect_encoding_and_delimiter
from quality_check import validate_data
from write_data import write_to_parquet, write_to_postgres

# Define file path for testing (SET IT HERE)
FILE_PATH = "/home/akshay/Iowa_Liquor_Sales.csv.csv"

@pytest.fixture(scope="session")
def spark():
    """
    Initializes a shared Spark session for testing.
    """
    return SparkSession.builder \
        .appName("Pytest-ETL") \
        .master("local[2]") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .getOrCreate()

# -------------------- Load Data Tests --------------------

def test_load_data(spark):
    """
    Test loading data from CSV.
    """
    df, _ = load_data(FILE_PATH)
    assert df is not None
    assert df.count() > 0  # Ensure data is loaded
    assert "store_location" in df.columns  # Ensure required column exists

def test_detect_encoding_and_delimiter():
    """
    Test detecting encoding and delimiter.
    """
    encoding, delimiter = detect_encoding_and_delimiter(FILE_PATH)
    assert encoding is not None
    assert delimiter == ","  # Assuming default delimiter is ','

def test_load_data_invalid_path():
    """
    Test load_data with an invalid file path.
    """
    invalid_path = "/invalid/path/to/file.csv"
    with pytest.raises(FileNotFoundError):
        load_data(invalid_path)

def test_load_data_empty_file(spark, tmp_path):
    """
    Test loading an empty CSV file.
    """
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("")  # Create an empty file

    with pytest.raises(ValueError, match="File is empty"):
        load_data(str(empty_csv))

def test_load_data_invalid_format(spark, tmp_path):
    """
    Test loading a file with an unsupported format.
    """
    invalid_file = tmp_path / "data.txt"
    invalid_file.write_text("sample text")  # Create a non-CSV file

    with pytest.raises(ValueError, match="Unsupported file format"):
        load_data(str(invalid_file))


# -------------------- Data Quality Checks --------------------

def test_data_quality(spark):
    """
    Test data validation and filtering.
    """
    df, _ = load_data(FILE_PATH)
    good_df, bad_df = validate_data(df)

    assert good_df is not None
    assert bad_df is not None
    assert good_df.count() + bad_df.count() == df.count()  # Ensure records are correctly classified

def test_data_quality_checks_failed(spark):
    """
    Test scenario where data validation fails for all records.
    """
    df, _ = load_data(FILE_PATH)

    # Set all values in 'store_location' to NULL
    df = df.withColumn("store_location", lit(None))

    good_df, bad_df = validate_data(df)

    assert bad_df.count() == df.count()  # Ensure all records are classified as bad

def test_load_data_all_good_records(spark, tmp_path):
    """
    Test case to verify that all good records are loaded correctly without any issues.
    """
    # Create a sample CSV file with only valid records
    valid_csv = tmp_path / "valid_records.csv"
    valid_csv.write_text(
        "invoice_and_item_number,date,store_number,store_location,category,category_name,vendor_name,item_number,item_description,pack,bottle_volume_ml,state_bottle_cost,state_bottle_retail,bottles_sold,sale_dollars,volume_sold_liters,volume_sold_gallons\n"
        "INV001,2025-03-15,1001,'(41.5868, -93.625)',101,Whiskey,Vendor1,5001,Whiskey A,6,750,10.00,15.00,20,300.00,15.0,3.96\n"
        "INV002,2025-03-16,1002,'(41.590, -93.620)',102,Vodka,Vendor2,5002,Vodka B,12,1000,8.00,12.00,50,600.00,50.0,13.20\n"
    )

    # Load the data
    df, _ = load_data(str(valid_csv))

    assert df.count() == 2
    assert "store_location" in df.columns
    assert df.filter(df.store_location.isNull()).count() == 0


def test_validate_data_fails(spark):
    """
    Tests that data validation fails when records contain invalid values.
    """

    # Creating a DataFrame with intentionally bad records
    invalid_data = [
        Row(store_location=None, invoice_and_item_number="12345", date="2025-03-19", sale_dollars=50.0, bottles_sold=5),
        Row(store_location="XYZ", invoice_and_item_number="12345", date="19-03-2025", sale_dollars=-10.0, bottles_sold=2),
        Row(store_location="ABC", invoice_and_item_number="67890", date="2025/03/19", sale_dollars=100.0, bottles_sold=0),
    ]

    df = spark.createDataFrame(invalid_data)

    # Run validation
    good_records_df, bad_records_df = validate_data(df)

    # Assertions
    assert bad_records_df is not None
    assert good_records_df.count() == 0
    assert bad_records_df.count() == 3

def test_data_quality_checks_missing_store_location(spark):
    """
    Test that records with missing store_location are filtered out.
    """
    data = [
        ("INV123", "2024-01-01", 100, 5, None),
        ("INV456", "2024-02-01", 200, 10, "Location2"),
    ]
    schema = "invoice_and_item_number STRING, date STRING, sale_dollars INT, bottles_sold INT, store_location STRING"
    df = spark.createDataFrame(data, schema)

    good_records, bad_records = validate_data(df)

    assert good_records.count() == 1  # One valid record
    assert bad_records.count() == 1  # One bad record

def test_data_quality_checks_invalid_date_format(spark):
    """
    Test that records with incorrect date format are filtered out.
    """
    data = [
        ("INV123", "01-01-2024", 100, 5, "Location1"),  # Invalid date format
        ("INV456", "2024-02-01", 200, 10, "Location2"),
    ]
    schema = "invoice_and_item_number STRING, date STRING, sale_dollars INT, bottles_sold INT, store_location STRING"
    df = spark.createDataFrame(data, schema)

    good_records, bad_records = validate_data(df)

    assert good_records.count() == 1  # One valid record
    assert bad_records.count() == 1  # One bad record

# -------------------- Write Data Tests --------------------

def test_write_parquet(spark, tmp_path):
    """
    Test writing bad records to Parquet.
    """
    df, _ = load_data(FILE_PATH)
    _, bad_df = validate_data(df)

    output_path = str(tmp_path / "bad_records.parquet")
    write_to_parquet(bad_df, output_path)

    assert os.path.exists(output_path)

def test_parquet_no_bad_records(spark, tmp_path):
    """
    Test Parquet writing with no bad records.
    """
    df, _ = load_data(FILE_PATH)
    good_df, bad_df = validate_data(df)
    bad_df = spark.createDataFrame([], df.schema)  # Simulate no bad records

    output_path = str(tmp_path / "empty_bad_records.parquet")
    write_to_parquet(bad_df, output_path)

    # Ensure that the Parquet file is not written for empty DataFrame
    assert not os.path.exists(output_path)

def test_write_postgres(spark):
    """
    Test writing good records to PostgreSQL.
    """
    df, _ = load_data(FILE_PATH)
    good_df, _ = validate_data(df)

    write_to_postgres(good_df, table_name="public.iowa_liquor_sales")

    assert good_df.count() > 0  # Ensure data was written

def test_postgres_write_failure(spark):
    """
    Test handling of PostgreSQL write failure.
    """
    df, _ = load_data(FILE_PATH)
    good_df, _ = validate_data(df)

    invalid_db_url = "jdbc:postgresql://invalid_host:5432/wrong_db"

    with pytest.raises(Exception):
        write_to_postgres(good_df, table_name="public.iowa_liquor_sales", db_url=invalid_db_url)

# -------------------- Run Tests --------------------

if __name__ == "__main__":
    pytest.main()
