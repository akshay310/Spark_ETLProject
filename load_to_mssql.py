"""
Connects the pyspark to the mssql and writes the good data into mssql
"""
import json
import logging
from pyspark.sql import DataFrame


# Configure logging
logger = logging.getLogger(__name__)
def load_db_config(config_path):
    '''
    Loads database configuration from a JSON file.
    Parameter:
        config_path (string):  contains the path to JSON configuration file.
    
    Returns:
        config (dict): Dictionary containing database connection details.

    Raises:
        Exception: If any error in loading the configuration.
    '''
    try:
        with open(config_path, "r", encoding = "utf-8") as file:
            config = json.load(file)
        logger.info("Database configuration loaded successfully.")
        return config
    except Exception as e:
        logger.exception("Failed to load database configuration: %s", e)
        raise

def write_to_mssql(df: DataFrame, db_config):
    """
    Writes a Spark DataFrame to a Microsoft SQL Server table using JDBC.

    Parameter:
        df (DataFrame): The DataFrame containing good records to be written to MSSQL.
        db_config (dict): Dictionary containing database connection details.

    Raises:
        Exception: If there is an error during data writing.
    """
    try:
        jdbc_url = (
            f"jdbc:sqlserver://{db_config['server']}:52237;"
            f"databaseName={db_config['database']};"
            f"encrypt=true;trustServerCertificate=true"
        )

        logger.info(
            "Writing data to MSSQL table: %s . %s",
             db_config["database"],
             db_config["table"]
             )

        # Writing the DataFrame to MSSQL using JDBC
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"dbo.{db_config['table']}") \
            .option("user", db_config["user"]) \
            .option("password", db_config["password"]) \
            .option("driver", db_config["driver"]) \
            .mode("append") \
            .save()

        logger.info("Successfully written %d records to %s", df.count(), db_config["table"])

    except Exception as e:
        logger.exception("Failed to write into MSSQL: %s", e)
        raise
   