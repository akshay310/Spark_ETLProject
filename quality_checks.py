"""
Performs data quality checks using Great Expectations.
The validation rules are dynamically loaded from a JSON configuration file.
"""

import logging
from pyspark.sql.functions import col, monotonically_increasing_id, when
from great_expectations.dataset import SparkDFDataset


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def apply_check(ge_df, df, rule):
    """
    Applies a single data quality check based on the rule configuration.

    Parameters:
        ge_df (SparkDFDataset): The Great Expectations DataFrame.
        df (DataFrame): The original Spark DataFrame.
        rule (dict): The rule from the JSON configuration.

    Returns:
        tuple: (check_success, failed_values)
    """
    column = rule["parameters"]["column"]
    check_type = rule["check"]
    check_success = True
    failed_values = []

    try:
        if check_type == "column_values_to_be_in_set":
            value_set = rule["parameters"]["value_set"]
            result = ge_df.expect_column_values_to_be_in_set(column, value_set)

        elif check_type == "column_values_to_be_unique":
            result = ge_df.expect_column_values_to_be_unique(column)

        elif check_type == "column_values_to_not_be_null":
            result = ge_df.expect_column_values_to_not_be_null(column)

        elif check_type == "column_values_to_be_between":
            min_val = rule["parameters"].get("min_value", None)
            max_val = rule["parameters"].get("max_value", None)
            strict_min = rule["parameters"].get("strict_min", False)

            # Handle dynamic calculations like "fare_amount * 0.5"
            if isinstance(max_val, str) and "*" in max_val:
                ref_col, multiplier = max_val.split(" * ")
                max_val = df.select(ref_col).first()[0] * float(multiplier)

            result = ge_df.expect_column_values_to_be_between(
                column, min_value=min_val, max_value=max_val, strict_min=strict_min
            )

        else:
            logger.warning("Unsupported check type: %s", check_type)
            return check_success, failed_values  # Skip unsupported checks

        if not result["success"]:
            check_success = False
            failed_values = result["result"]["partial_unexpected_list"]

    except Exception as e:
        logger.exception("Error applying check '%s' on column '%s': %s", check_type, column, e)
        check_success = False

    return check_success, failed_values



def quality_checks(df, config):
    """
    Applies data quality checks based on JSON configuration.

    Parameters:
        df (DataFrame): The input Spark DataFrame.
        config (dict): The loaded configuration dictionary.

    Returns:
        tuple: (good_data, bad_data)
    """
    try:
        logger.info("Starting data quality checks...")
        df = df.withColumn("row_index", monotonically_increasing_id())
        ge_df = SparkDFDataset(df)

        failed_conditions = []

        for rule in config["data_quality"]:
            check_success, failed_values = apply_check(ge_df, df, rule)
            if not check_success:
                failed_conditions.append((rule["parameters"]["column"], failed_values))

        # Flagging failed records
        validation_status = df.withColumn(
                    "validation_status", 
                    when(col("row_index").isNotNull(),
                    "PASS"))

        for col_name, failed_values in failed_conditions:
            validation_status = validation_status.withColumn(
                "validation_status",
                when((col(col_name).isin(failed_values)) | (col("validation_status") == "FAIL"),
                "FAIL").otherwise("PASS")
            )

        good_records = (
            validation_status
            .filter(col("validation_status") == "PASS")
            .drop("validation_status")
        )

        bad_records = (
            validation_status
            .filter(col("validation_status") == "FAIL")
            .drop("validation_status")
        )

        logger.info("Total records: %d", df.count())
        logger.info("Good records: %d", good_records.count())
        logger.info("Bad records: %d", bad_records.count())

        # Save results as Parquet files
        bad_records_path = (
            f"{config['target']['bad_record_file_path']}/"
            f"{config['target']['bad_record_file_name']}"
        )

        good_records_path = (
            f"{config['target']['good_record_file_path']}/"
            f"{config['target']['good_record_file_name']}"
        )


        bad_records.write.mode("overwrite").parquet(bad_records_path)
        good_records.write.mode("overwrite").parquet(good_records_path)

        logger.info("Good records saved at: %s", good_records_path)
        logger.info("Bad records saved at: %s", bad_records_path)

        return good_records, bad_records

    except Exception as e:
        logger.exception("Error during data quality checks: %s", str(e))
        raise
