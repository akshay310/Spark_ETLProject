"""
Main module for orchestrating the ETL pipeline.
"""
import json
import logging
import os
from load_data import load_data
from data_quality import validate_and_clean_data
from save_data import save_data
from load_to_oracle import save_to_oracle
from create_table import create_table_if_not_exists
from pyspark.sql.functions import col

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(filename='etl.log',level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def load_config():
    """Loads and validates configuration."""
    config_path = os.getenv("CONFIG_PATH", "config.json")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        config = json.load(f)

    required_keys = ["etl_config"]
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing key in config: {key}")

    return config

def main(config):
    """
    Executes the ETL pipeline:
    1. Loads data from CSV into PySpark DataFrame.
    2. Cleans and validates the data.
    3. Saves bad records to Parquet.
    4. Loads good records into Oracle.

    Args:
        config (dict): Configuration dictionary.
    """
    logger.info("Starting ETL process.")

    # Extract configuration values
    etl_config = config["etl_config"]
    file_path = etl_config["source"].get("file_path")
    table_name = etl_config["target"].get("table_name")
    bad_records_path = etl_config["target"].get("bad_records_path")

    if not all([file_path, table_name, bad_records_path]):
        raise ValueError("Invalid ETL config: Ensure file_path, table_name, and bad_records_path are defined.")

    # Load Data
    logger.info(f"Loading data from {file_path}...")
    df = load_data(file_path, config)

    # Validate & Clean Data
    logger.info("Validating and cleaning data...")
    good_records_df, bad_records_df = validate_and_clean_data(df, config)
    logger.warning(f"Found {bad_records_df.count()} bad records")
    logger.info(f"{good_records_df.count()} valid records found to load into oracle database:{table_name}")
    # # Save Bad Records
    # if bad_records_df and bad_records_df.count() > 0:
    #     logger.warning(f"Found {bad_records_df.count()} bad records. Saving to {bad_records_path}...")
    #     save_data(good_records_df, bad_records_df, bad_records_path)

    # # Create Oracle table if not exists
    # logger.info(f"Ensuring table {table_name} exists in Oracle DB...")
    # create_table_if_not_exists(config)

    # # Load to Oracle
    # logger.info(f"Loading {good_records_df.count()} valid records to Oracle table: {table_name}")
    # save_to_oracle(good_records_df, table_name)

    logger.info("ETL process completed successfully.")

if __name__ == "__main__":
    config = load_config()
    main(config)
