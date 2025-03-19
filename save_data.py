"""
Module for handling saving of data.
"""
import logging
from pyspark.sql import DataFrame

def save_data(good_records_df: DataFrame, bad_records_df: DataFrame, bad_records_path: str):
    """
    Saves bad records to Parquet and prepares good records for Oracle ingestion.

    Args:
        good_records_df (DataFrame): Validated DataFrame.
        bad_records_df (DataFrame): Invalid records DataFrame.
        bad_records_path (str): Path to save bad records.
    """
    bad_records_count = bad_records_df.count()
    good_records_count = good_records_df.count()

    if bad_records_count > 0:
        bad_records_df.write.mode("overwrite").parquet(bad_records_path)
        logging.info("Saved %d bad records to %s.", bad_records_count, bad_records_path)

    logging.info("%d good records ready for Oracle ingestion.", good_records_count)
