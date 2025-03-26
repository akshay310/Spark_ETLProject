"""
Module for data validation using Great Expectations.
"""
import logging
import re
from datetime import datetime
import great_expectations as ge
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def sanitize_column_name(col_name: str) -> str:
    """Replaces special characters in column names with underscores for Oracle compatibility."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", col_name.upper())

def format_review_time(review_time):
    """
    Converts timestamps to Oracle-compatible format or returns None for invalid values.

    Args:
        review_time: Timestamp value.

    Returns:
        str: Formatted date or None for invalid timestamps.
    """
    try:
        timestamp = int(float(review_time))
        return (
            datetime.utcfromtimestamp(timestamp).strftime("%d-%b-%Y %H:%M:%S")
            if timestamp >= 0 else None
        )
    except (ValueError, TypeError):
        return None

def clean_value(value, max_length=255):
    """
    Cleans data values before insertion into Oracle.

    Args:
        value: Any data value.
        max_length (int): Maximum allowed length for string values.

    Returns:
        Cleaned value.
    """
    if isinstance(value, str):
        return value[:max_length]
    if isinstance(value, (int, float)):
        return value
    return None

def validate_and_clean_data(df: DataFrame,config):
    """
    Performs data quality checks, cleans data, and returns good and bad records.

    Args:
        df (DataFrame): Input PySpark DataFrame.

    Returns:
        Tuple[DataFrame, DataFrame]: (good_records_df, bad_records_df)
    """
    logging.info("Performing data validation and cleaning...")

    expected_columns = [
        check["column"] 
        for check in config["etl_config"]["data_quality_checks"]
        ]
    df = df.select(*[c for c in expected_columns if c in df.columns])  # Select required columns

    # Great Expectations validation
    df_ge = ge.dataset.SparkDFDataset(df)
    validation_results = {}

    for check in config["etl_config"]["data_quality_checks"]:
        column = check["column"]
        check_type = check["check"]

        if check_type == "not_null":
            validation_results[column] = df_ge.expect_column_values_to_not_be_null(column)
        elif check_type == "greater_than_equal":
            validation_results[column] = df_ge.expect_column_values_to_be_between(
                column, min_value=check["value"]
            )
        elif check_type == "regex_match":
            validation_results[column] = df_ge.expect_column_values_to_match_regex(
                column, check["pattern"]
            )

    for column, result in validation_results.items():
        logging.info("Validation on column '%s': %s", column, result["success"])

    failed_columns = [col for col, result in validation_results.items() if not result["success"]]
    
    if failed_columns:
        bad_records_df = df.filter(
            (col(failed_columns[0]).isNull()) | (col(failed_columns[0]) == "")
        )
        for col_name in failed_columns[1:]:
            bad_records_df = bad_records_df.union(df.filter(
                col(col_name).isNull() | (col(col_name) == "")
            ))
        good_records_df = df.subtract(bad_records_df)
    else:
        good_records_df = df
        bad_records_df = df.limit(0)
    
    logging.info("Data quality validation completed successfully")


    
    good_records_df = good_records_df.withColumn("Title", col("Title").substr(1, 255))
    good_records_df = good_records_df.withColumn("review/time", col("review/time").substr(1, 255))
    good_records_df = good_records_df.withColumn("review/score", col("review/score").substr(1, 255))
    good_count=good_records_df.count()
    bad_count=bad_records_df.count()
    logging.info("Valid records:%d,Invalid records:%d",good_count,bad_count)
    return good_records_df, bad_records_df
