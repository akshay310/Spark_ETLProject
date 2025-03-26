"""
Module to write good records into PostgreSQL and bad records into a Parquet file.

This module:
- Writes invalid (bad) records to a Parquet file.
- Writes valid (clean) records to a PostgreSQL database.

It reads configuration values from `config.json`, and uses credentials from `db_cred.py`.

Usage:
    python write_data.py
"""

import logging
import json
from pyspark.sql import DataFrame
from db_cred import DB_URL, DB_PROPERTIES

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load configuration
with open("config.json", "r", encoding='UTF-8') as config_file:
    config = json.load(config_file)

db_config = config["database"]
target_config = config["task"]["target"]

PARQUET_OUTPUT_PATH = target_config["parquet_output_path"]
POSTGRES_TABLE_NAME = db_config["postgres_table_name"]


def write_to_parquet(input_df: DataFrame, output_path: str):
    """
    Writes a DataFrame containing bad records to a Parquet file.

    Args:
        input_df (DataFrame): PySpark DataFrame containing bad or invalid records.
        output_path (str): Absolute path where the Parquet file should be saved.

    Raises:
        Exception: If the write operation fails.
    """
    try:
        if not input_df.isEmpty():
            record_count = input_df.count()
            logging.info("Writing %s records to parquet file at %s", record_count, output_path)
            input_df.write.mode("overwrite").parquet(output_path)
            logging.info("Bad records successfully written to: %s", output_path)
        else:
            logging.info("No bad records found, skipping Parquet write.")
    except Exception as error:
        logging.error("Failed to write bad records to Parquet: %s", str(error))
        raise


def write_to_postgres(input_df: DataFrame, table_name: str):
    """
    Writes a clean DataFrame to a PostgreSQL table using JDBC.

    Args:
        input_df (DataFrame): PySpark DataFrame containing valid records.
        table_name (str): Fully qualified table name in PostgreSQL (e.g., 'public.my_table').

    Raises:
        Exception: If the write operation to PostgreSQL fails.
    """
    try:
        logging.info("Good Records Schema: %s", input_df.schema)
        record_count = input_df.count()
        logging.info("Good Records Count: %s", record_count)
        logging.info("Writing %s records to PostgreSQL table: %s", record_count, table_name)

        input_df.write \
            .jdbc(url=DB_URL, table=table_name, mode="overwrite", properties=DB_PROPERTIES)

        logging.info("Good records successfully written to PostgreSQL!")
    except Exception as error:
        logging.error("Failed to write data to PostgreSQL: %s", str(error))
        raise


if __name__ == "__main__":
    from load_data import load_data
    from quality_check import validate_data

    FILE_PATH = config["task"]["source"]["file_path"] + config["task"]["source"]["file_name"]

    # Load data and retrieve the SparkSession
    data_df, spark_session = load_data(FILE_PATH)

    # Validate and separate records
    good_records_df, bad_records_df = validate_data(data_df)

    # Write bad records to Parquet file
    if bad_records_df:
        write_to_parquet(bad_records_df, output_path=PARQUET_OUTPUT_PATH)

    # Write good records to PostgreSQL
    write_to_postgres(good_records_df, table_name=POSTGRES_TABLE_NAME)
