"""
Module: write_parquet

This module provides a function to 
write a PySpark DataFrame to a Parquet 
file with exception handling and logging.
"""
import logging
from pyspark.sql import DataFrame

def write_parquet(bad_records: DataFrame, file_path: str):
    """Writes a PySpark DataFrame to a Parquet file with exception handling and logging."""
    try:
        logging.info("Starting to write DataFrame to Parquet at: %s", file_path)
        bad_records.write.format("parquet").mode("overwrite").save(file_path)
        logging.info("Data successfully written to Parquet at: %s", file_path)
    except Exception as e:
        logging.error("Error writing DataFrame to Parquet: %s", str(e))
        raise
