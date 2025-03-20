"""
Spark MySQL Integration Module

This module provides functions to interact with Apache Spark and MySQL.  
It includes functionalities for initializing a Spark session, reading JSON data,
    and writing a PySpark DataFrame to a MySQL database.  
Logging and exception handling are integrated for better debugging and reliability.

Functions:
- start_spark(app_name="DQ"): Initializes and
    returns a SparkSession with the given application name.
- read_json_data(spark, file): Reads a JSON file into a Spark DataFrame with schema inference.
- write_to_mysql(df, url, dbtable, user, password): Writes a PySpark DataFrame to a MySQL table.
"""

import logging
from pyspark.sql import DataFrame

def write_to_mysql(df: DataFrame, url, dbtable, user, password):
    """Writes a PySpark DataFrame to a MySQL database with exception handling and logging."""
    try:
        logging.info("Starting data write to MySQL table: %s", dbtable)
        df.write.format("jdbc") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("url", url) \
            .option("dbtable", dbtable) \
            .option("user", user) \
            .option("password", password) \
            .save()
        logging.info("Data successfully written to MySQL")
    except Exception as e:
        logging.error("Error writing data to MySQL: %s", str(e))
        raise
