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

    # Check for missing credentials
    if not all([ORACLE_URL, ORACLE_USER, ORACLE_PASSWORD, ORACLE_DRIVER]):
        logging.error("Missing Oracle database credentials. Please check environment variables.")
        raise ValueError("Missing Oracle credentials.")

    # Check if DataFrame is empty before writing
    if df.isEmpty():
        logging.warning("DataFrame is empty. No data written to Oracle table: %s", table_name)
        return

    try:
        logging.info("Saving DataFrame to Oracle table: %s", table_name)
        df.write \
            .format("jdbc") \
            .option("url", ORACLE_URL) \
            .option("dbtable", table_name) \
            .option("user", ORACLE_USER) \
            .option("password", ORACLE_PASSWORD) \
            .option("driver", ORACLE_DRIVER) \
            .option("sessionInitStatement", "ALTER SESSION SET ISOLATION LEVEL READ COMMITTED") \
            .mode("append") \
            .save()
        logging.info("Data successfully saved to Oracle table: %s", table_name)

    except AnalysisException as e:
        logging.error("Spark SQL AnalysisException: %s", str(e))
    except ValueError as e:
        logging.error("ValueError (Invalid Data): %s", str(e))
    except ConnectionError as e:
        logging.error("ConnectionError: Could not connect to Oracle DB: %s", str(e))
    except Exception as e:
        logging.error("Unexpected error while saving to Oracle: %s", str(e))
    except py4j.protocol.Py4JJavaError as e:
        logging.error("PySpark Error: %s", str(e))
    except Exception as e:
        logging.error("Unexpected Error: %s", str(e))

