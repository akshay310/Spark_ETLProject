#main.py
import logging
from load_data import load_data
from data_quality import validate_and_clean_data
from save_data import save_data
from load_to_oracle import save_to_oracle

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main(file_path: str, table_name: str, bad_records_path: str):
    """
    Executes the ETL pipeline:
    1. Loads data from CSV into PySpark DataFrame.
    2. Cleans and validates the data.
    3. Saves bad records to Parquet.
    4. Loads good records into Oracle.
    
    Args:
        file_path (str): Path to the CSV file.
        table_name (str): Name of the Oracle table.
        bad_records_path (str): Path to store bad records.
    """
    logging.info("Starting ETL process.")

    # Load Data
    df = load_data(file_path)

    # Validate & Clean Data
    good_records_df, bad_records_df = validate_and_clean_data(df)

    # Save Data
    save_data(good_records_df, bad_records_df, bad_records_path)

    # Load to Oracle
    save_to_oracle(good_records_df, table_name)

    logging.info("ETL process completed successfully.")

if __name__ == "__main__":
    FILE_PATH = "/home/writv/pyspark_etl/dataset/Books_rating.csv"
    TABLE_NAME = "BOOKRATINGS"
    BAD_RECORDS_PATH = "/home/writv/pyspark_etl/bad_data/to/validation_results.parquet"

    main(FILE_PATH, TABLE_NAME, BAD_RECORDS_PATH)
