"""
Spark Data Json Reading Module

This module provides functions to initialize a Spark session 
    and read JSON data into a Spark DataFrame.  
It includes logging for debugging and error handling to ensure smooth execution.

Functions:
- start_spark(app_name="DQ"): Initializes and 
    returns a SparkSession with a specified application name.
- read_json_data(spark, file): Reads a JSON file into a Spark DataFrame with schema inference.
"""

import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def start_spark(app_name="DQ"):
    """
    Initializes and returns a Spark session.
    
    :param app_name: Name of the Spark application
    :return: SparkSession object
    """
    connector_path = "/home/reyona/pyproj/pyspark_proj_env/mysql-connector-j-9.2.0.jar"
    try:
        spark = (SparkSession.builder
            .appName(app_name)
            .config("spark.jars", connector_path)
            .master("local[*]")
            .getOrCreate()
        )
        logging.info("Spark session started successfully.")
        return spark
    except Exception as e:
        logging.error("Error initializing Spark session: %s", e)
        raise

def read_json_data(spark, file):
    """
    Reads a JSON file into a Spark DataFrame.
    
    :param spark: Spark session
    :param file: Path to the JSON file
    :return: Spark DataFrame
    """
    try:
        df = spark.read.format("json").option("inferSchema", True).load(file)
        logging.info("Successfully read JSON file: %s", file)
        return df
    except FileNotFoundError:
        logging.error("File not found: %s",file)
        raise
    except Exception as e:
        logging.error("Error reading JSON file %s: %s", file, e)
        raise
