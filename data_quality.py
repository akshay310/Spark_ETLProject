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

def validate_and_clean_data(df: DataFrame):
    """
    Performs data quality checks, cleans data, and returns good and bad records.

    Args:
        df (DataFrame): Input PySpark DataFrame.

    Returns:
        Tuple[DataFrame, DataFrame]: (good_records_df, bad_records_df)
    """
    logging.info("Performing data validation and cleaning...")

    expected_columns = [
        "Id", "Title", "review/score", "Price", "review/time", "review/helpfulness"
    ]
    df = df.select(*[c for c in expected_columns if c in df.columns])  # Select required columns

    # Great Expectations validation
    df_ge = ge.dataset.SparkDFDataset(df)
    validation_checks = {
        "Id": df_ge.expect_column_values_to_not_be_null("Id"),
        "Title": df_ge.expect_column_values_to_not_be_null("Title"),
        "review/score": df_ge.expect_column_values_to_not_be_null("review/score"),
        "Price": df_ge.expect_column_values_to_be_between("Price", min_value=0),
        "review/time format": df_ge.expect_column_values_to_match_regex("review/time", r"\d+"),
        "review/helpfulness format": df_ge.expect_column_values_to_match_regex(
            "review/helpfulness", r"\d+/\d+"
        )
    }

    for check, result in validation_checks.items():
        logging.info("%s validation: %s", check, result["success"])

    df = df.withColumn(
        "is_valid",
        when(
            col("Id").isNotNull()
            & col("Title").isNotNull()
            & col("review/score").isNotNull()
            & (col("Price") >= 0)
            & col("review/time").rlike(r"\d+")
            & col("review/helpfulness").rlike(r"\d+/\d+"),
            True
        ).otherwise(False)
    )

    good_records_df = df.filter(col("is_valid")).drop("is_valid")
    bad_records_df = df.filter(~col("is_valid")).drop("is_valid")

    logging.info("Valid records: %d, Invalid records: %d", good_records_df.count(), bad_records_df.count())
    return good_records_df, bad_records_df
