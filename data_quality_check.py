"""
Module: data_quality_validation  
================================  

This module provides functions for validating data quality in PySpark DataFrames using  
Great Expectations. It ensures data consistency, uniqueness, and correctness based on  
predefined validation rules.  

### Functions:
- `get_date_columns(df)`: Identifies and returns a list of date-related column names in a DataFrame.  
- `validate_data_quality(spark_df, required_columns, allowed_values,
                    required_datatypes, unique_values)`:  
  Performs data quality validation by checking for null values, allowed value sets,
    expected data types, and uniqueness constraints.  

### Features:
- Uses **Great Expectations** to define and apply validation rules.  
- Identifies columns containing dates for additional validation.  
- Separates valid and invalid records into **good_records** and **bad_records** DataFrames.  
- Logs validation progress and errors for easier debugging.
"""
import logging
from great_expectations.dataset import SparkDFDataset
from pyspark.sql.functions import col

def get_date_columns(df):
    """Returns a list of date column names."""
    date_cols = []
    fields = df.schema.fields
    for field in fields:
        column_name = field.name
        if "date" in column_name:
            date_cols.append(column_name)
    return date_cols

def validate_data_quality(spark_df, required_columns, \
                          allowed_values, required_datatypes, unique_values):
    """
    Validates data quality using Great Expectations, ensuring data consistency,
    uniqueness, and type correctness.

    :param spark_df: The input Spark DataFrame.
    :param required_columns: List of columns that should not contain null values.
    :param allowed_values: Dictionary mapping column names to lists of allowed values.
    :param required_datatypes: Dictionary mapping column names to expected data types.
    :param unique_values: List of columns that should contain unique values.
    :return: A tuple containing two Spark DataFrames:
             - good_records: DataFrame with valid records.
             - bad_records: DataFrame with invalid records.
    :raises Exception: If an error occurs during validation.
    """
    try:
        logging.info("Starting data quality validation")
        df_ge = SparkDFDataset(spark_df)
        expectations = []
        for column in required_columns:
            expectations.append(df_ge.expect_column_values_to_not_be_null(column))
        date_columns = get_date_columns(spark_df)
        for date_col in date_columns:
            expectations.append(df_ge.expect_column_values_to_match_regex(date_col,
                                r"\w{3} \d{1,2} \d{4}"))
        for column, values in allowed_values.items():
            expectations.append(df_ge.expect_column_values_to_be_in_set(column, values))
        for column, data_type in required_datatypes.items():
            expectations.append(df_ge.expect_column_values_to_be_of_type(column, data_type))
        for column in unique_values:
            expectations.append(df_ge.expect_column_values_to_be_unique(column))
        failed_conditions = []
        for expectation in expectations:
            if not expectation["success"]:
                failed_conditions.append(expectation["expectation_config"]["kwargs"]["column"])
        if failed_conditions:
            bad_records = spark_df.filter(
                (col(failed_conditions[0]).isNull()) | (col(failed_conditions[0]) == "")
            )
            for col_name in failed_conditions[1:]:
                bad_records = bad_records.union(spark_df.filter(
                    col(col_name).isNull() | (col(col_name) == "")
                ))
            good_records = spark_df.subtract(bad_records)
        else:
            good_records = spark_df
            bad_records = spark_df.limit(0)
        logging.info("Data quality validation completed successfully")
        return good_records, bad_records
    except Exception as e:
        logging.error(f"Error during data quality validation: {e}")
        raise
