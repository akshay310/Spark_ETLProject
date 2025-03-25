"""
Main module for orchestrating the ETL pipeline.
"""
import json
import logging
from load_data import load_data
from data_quality import validate_and_clean_data
from save_data import save_data
from load_to_oracle import save_to_oracle
from create_table import create_table_if_not_exists
from pyspark.sql.functions import col

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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
    logging.info("Starting ETL process.")

    # Extract configuration values
    file_path = config["etl_config"]["source"]["file_path"]
    table_name = config["etl_config"]["target"]["table_name"]
    bad_records_path = config["etl_config"]["target"]["bad_records_path"]

    # Load Data
    df = load_data(file_path, config)

    # Validate & Clean Data
    good_records_df, bad_records_df = validate_and_clean_data(df, config)

    # Save Data
    save_data(good_records_df, bad_records_df, bad_records_path)
    
    # Create Oracle table if not exists (with extended Title column length)
    create_table_if_not_exists(config)

    good_records_df = good_records_df.withColumn("Title", col("Title").substr(1, 255))
    logging.info("Restricted Title column to 255 characters.")
    # Load to Oracle
    save_to_oracle(good_records_df, table_name)

    logging.info("ETL process completed successfully.")

if __name__ == "__main__":
    with open("config.json", "r") as f:
        config = json.load(f)
    main(config)
