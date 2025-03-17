#save_data.py
from pyspark.sql import DataFrame
import logging

def save_data(good_records_df: DataFrame, bad_records_df: DataFrame, bad_records_path: str):
    """
    Saves bad records to Parquet and prepares good records for Oracle ingestion.

    Args:
        good_records_df (DataFrame): Validated DataFrame.
        bad_records_df (DataFrame): Invalid records DataFrame.
        bad_records_path (str): Path to save bad records.
    """
    if bad_records_df.count() > 0:
        bad_records_df.write.mode("overwrite").parquet(bad_records_path)
        logging.info(f"Saved {bad_records_df.count()} bad records to {bad_records_path}.")

    logging.info(f"{good_records_df.count()} good records ready for Oracle ingestion.")
