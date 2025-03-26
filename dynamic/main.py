"""
Main Script to run the ETL Pipeline.

This script performs the following:
1. Loads configuration from config.json.
2. Reads CSV data using PySpark.
3. Validates data using Great Expectations.
4. Writes bad records to a Parquet file.
5. Writes clean data to a PostgreSQL table.

Usage:
    python main.py
"""

import logging
import json
from load_data import load_data
from quality_check import validate_data
from write_data import write_to_parquet, write_to_postgres

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load configuration from config.json
with open("config.json", "r", encoding='UTF-8') as config_file:
    config = json.load(config_file)

if __name__ == "__main__":
    try:
        # Extract parameters from config
        file_path = config["task"]["source"]["file_path"] + config["task"]["source"]["file_name"]
        parquet_output_path = config["task"]["target"]["parquet_output_path"]
        postgres_table_name = config["database"]["postgres_table_name"]

        # Step 1: Load data and retrieve the SparkSession
        df, spark = load_data(file_path)

        # Step 2: Validate and separate records into good and bad
        good_records_df, bad_records_df = validate_data(df)

        # Step 3: Write bad records to Parquet file
        if bad_records_df:
            write_to_parquet(bad_records_df, output_path=parquet_output_path)

        # Step 4: Write good records to PostgreSQL
        write_to_postgres(good_records_df, table_name=postgres_table_name)

        logging.info("✅ ETL pipeline completed successfully!")

    except Exception as e:
        logging.error("❌ ETL pipeline failed: %s", str(e))
