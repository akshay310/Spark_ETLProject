"""
Creates spark session and reads the data
"""

import logging
from pyspark.sql import SparkSession


# Configure logging
logger = logging.getLogger(__name__)

def create_spark_session():
    """
    Creates and returns a Spark session.

    Returns:
        SparkSession: A configured Spark session.
    """
    try:
        logger.info("Creating Spark session.")

        spark = SparkSession.builder \
            .appName("NYCTaxiDataReader") \
            .config("spark.driver.extraClassPath",
             "/home/vaishnavi/nyc_taxi_pyspark/mssql-jdbc-12.8.1.jre11.jar")\
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.memory.fraction", "0.6") \
            .config("spark.memory.storageFraction", "0.3") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        return spark
    except Exception as e:
        logger.exception("Failed to create Spark session: %s", e)
        raise

def read_parquet_data(spark: SparkSession, input_path: str):
    """
    Reads Parquet files from the specified input path using a given Spark session.

    Parameters:
        spark (SparkSession): The active Spark session.
        input_path (str): The directory or file pattern for the Parquet files.

    Returns:
        DataFrame: A Spark DataFrame containing the taxi trip data.

    Raises:
        Exception: If there is an error reading the Parquet files.
    """
    try:
        logger.info("Reading Parquet data from '%s'.", input_path)
        df = spark.read.parquet(input_path)
        logger.info("Successfully read parquet data from '%s'.", input_path)
        row_count = df.count()
        logger.info("Total number of rows in the dataset: %d", row_count)
        logger.info("Showing first 5 rows of the dataset:")
        df.show(5)
        return df
    except Exception as e:
        logger.exception("Failed to read parquet data from '%s': %s", input_path, e)
        raise
