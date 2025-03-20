"""
Connects the pyspark to the mssql and writes the good data into mssql
"""
import logging
import os
from pyspark.sql import DataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

 # Fetch database credentials from environment variables
db_server = os.getenv("DB_SERVER")
db_port = os.getenv("DB_PORT")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_driver = os.getenv("DB_DRIVER")



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
                    f"jdbc:sqlserver://{db_server}:{db_port};"
                    f"databaseName={db_config['database']['db_name']};"
                    "encrypt=true;trustServerCertificate=true"
                    )


    #    logger.info(
    #         "Writing data to MSSQL table: %s . %s",
    #          db_config["database"]['database'],
    #          db_config['database']["table"]
    #          )

        # Writing the DataFrame to MSSQL using JDBC
        logger.info(
            "Writing data to MSSQL table: %s.%s",
            db_config["database"]["db_name"],
            db_config["database"]["db_table"]
            )

        # Writing the DataFrame to MSSQL using JDBC
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"dbo.{db_config['database']['db_table']}") \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", db_driver) \
            .mode("append") \
            .save()

        logger.info(
            "Successfully written %d records to %s",
            df.count(),
            db_config['database']['db_table']
            )

    except Exception as e:
        logger.exception("Failed to write into MSSQL: %s", str(e))
        raise
