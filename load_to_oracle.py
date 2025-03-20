"""
Module to load data into Oracle Database.
"""
import os
import logging
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Oracle Database Connection Details
ORACLE_URL = os.getenv("ORACLE_URL")
ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_DRIVER = os.getenv("ORACLE_DRIVER")

def save_to_oracle(df: DataFrame, table_name: str) -> None:
    """
    Saves a cleaned and validated PySpark DataFrame to an Oracle database.

    Args:
        df (DataFrame): The DataFrame containing valid records.
        table_name (str): The target Oracle table name.
    """
    try:
        logging.info("Saving DataFrame to Oracle database...")
        df.write \
            .format("jdbc") \
            .option("url", ORACLE_URL) \
            .option("dbtable", table_name) \
            .option("user", ORACLE_USER) \
            .option("password", ORACLE_PASSWORD) \
            .option("driver", ORACLE_DRIVER) \
            .option("sessionInitStatement", "ALTER SESSION SET ISOLATION LEVEL READ COMMITTED") \
            .mode("overwrite") \
            .save()
        logging.info("Data successfully saved to Oracle table: %s", table_name)
    except AnalysisException as e:
        logging.error("AnalysisException encountered: %s", str(e))
    except (ValueError, ConnectionError, OSError) as e:
        logging.error("Database operation error: %s", str(e))