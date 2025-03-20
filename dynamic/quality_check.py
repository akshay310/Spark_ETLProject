"""
Module to perform data quality checks using Great Expectations.
"""
import json
import logging
import great_expectations as ge
from pyspark.sql import DataFrame
from load_data import load_data

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load config.json
with open("config.json", "r",encoding='UTF-8') as config_file:
    config = json.load(config_file)

def validate_data(df: DataFrame):
    """
    Performs data quality checks based on config.json and returns good and bad records.
    """
    try:
        df_ge = ge.dataset.SparkDFDataset(df)
        expectations_config = config["task"]["data_quality"]["expectations"]
        failed_conditions = []
        for check_name, check_details in expectations_config.items():
            column = check_details["column"]
            expectation_type = check_details["great_expectations"]
            if expectation_type == "expect_column_values_to_not_be_null":
                result = df_ge.expect_column_values_to_not_be_null(column)
            elif expectation_type == "expect_column_values_to_be_unique":
                result = df_ge.expect_column_values_to_be_unique(column)
            elif expectation_type == "expect_column_values_to_match_regex":
                result = df_ge.expect_column_values_to_match_regex(column, check_details["regex"])
            elif expectation_type == "expect_column_values_to_be_between":
                result = df_ge.expect_column_values_to_be_between(column,check_details.get("min"),
                check_details.get("max"))
            else:
                logging.warning("Unknown expectation type: %s", expectation_type)
                continue

            if result["success"]:
                logging.info("%s check PASSED.", check_name)
            else:
                logging.warning("%s check FAILED.", check_name)
                failed_conditions.append(check_details["sql_condition"])

        # Separate good and bad records
        if failed_conditions:
            filter_condition = " OR ".join(f"NOT ({condition})" for condition in failed_conditions)
            bad_records_df = df.filter(filter_condition)
            good_records_df = df.subtract(bad_records_df)
            logging.info("Bad records found. Good: %s | Bad: %s",good_records_df.count(), bad_records_df.count())
        else:
            good_records_df = df
            bad_records_df = None
            logging.info("All records passed data quality checks.")
        return good_records_df, bad_records_df
    except Exception as error:
        logging.error("Data validation failed: %s", str(error))
        raise

if __name__ == "__main__":
    file_path = config["task"]["source"]["file_path"] + config["task"]["source"]["file_name"]
    data_df, spark_session = load_data(file_path)
    good_records_df, bad_records_df = validate_data(data_df)

    good_records_df.show(5)
    if bad_records_df:
        bad_records_df.show(5)
        bad_records_df.write.mode("overwrite").parquet("/home/akshay/bad_records.parquet")
        logging.info("Bad records successfully written to Parquet.")
        logging.info("Good records are ready to be pushed to PostgreSQL database")
