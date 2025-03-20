"""
Module to load data into Oracle Database.
"""
import json
import os
import logging
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

# # Load configuration
# with open("config.json", "r") as f:
#     config = json.load(f)


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def save_to_oracle(df: DataFrame, table_name: str,config) -> None:
    """
    Saves a cleaned and validated PySpark DataFrame to an Oracle database.

    Args:
        df (DataFrame): The DataFrame containing valid records.
        table_name (str): The target Oracle table name.
    """
    
    # Oracle Database Connection Details
    ORACLE_CONFIG = config["etl_config"]["oracle_connection"]
    try:
        logging.info("Saving DataFrame to Oracle database...")
        df.write \
            .format("jdbc") \
            .option("url", ORACLE_CONFIG["url"]) \
            .option("dbtable", table_name) \
            .option("user", ORACLE_CONFIG["user"]) \
            .option("password", ORACLE_CONFIG["password"]) \
            .option("driver",  ORACLE_CONFIG["driver"]) \
            .option("sessionInitStatement", ORACLE_CONFIG["sessionInitStatement"]) \
            .mode("append") \
            .save()
        logging.info("Data successfully saved to Oracle table: %s", table_name)
    except AnalysisException as e:
        logging.error("AnalysisException encountered: %s", str(e))
    except (ValueError, ConnectionError, OSError) as e:
        logging.error("Database operation error: %s", str(e))
