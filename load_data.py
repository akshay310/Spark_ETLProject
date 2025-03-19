"""
Module to load CSV data into a PySpark DataFrame with automatic delimiter and encoding detection.
"""

import logging
import csv
import os
import chardet
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)


def detect_encoding_and_delimiter(file_path, sample_size=10000):
    """
    Detects the file encoding and delimiter.

    :param file_path: Path to the CSV file
    :param sample_size: Number of bytes to read for encoding detection
    :return: Tuple (encoding, delimiter)
    """
    with open(file_path, "rb") as file:
        raw_data = file.read(sample_size)
        encoding_info = chardet.detect(raw_data)
        encoding = encoding_info.get("encoding", "utf-8")  # Default to UTF-8 if None

        # Detect delimiter
    with open(file_path, "r", encoding=encoding) as file:
        sample = file.readline()
        delimiter = csv.Sniffer().sniff(sample).delimiter

    logging.info("Detected Encoding: %s", encoding)
    logging.info("Detected Delimiter: %s", delimiter)

    return encoding, delimiter

def get_spark_session(app_name="ETL-Load-Data"):
    """
    Initializes and returns a Spark session.
    """
    try:
        spark = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memoryOverhead", "1g")
            .config("spark.memory.fraction", "0.8")
            .config("spark.memory.storageFraction", "0.5")
            .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")  # Suppress WARN messages

        logging.info("Spark session created successfully.")
        return spark
    except Exception as error:
        logging.error("Error creating Spark session: %s", str(error))
        raise

def load_data(file_path):
    """
    Loads a CSV file into a PySpark DataFrame with detected encoding and delimiter.

    :param file_path: Path to the CSV file
    :return: Tuple (DataFrame, Spark Session)
    """
    try:
        if not os.path.exists(file_path):
            logging.error("File not found: %s", file_path)
            raise FileNotFoundError(f"File not found: {file_path}")

        if os.stat(file_path).st_size == 0:
            logging.error("File is empty: %s", file_path)
            raise ValueError(f"File is empty: {file_path}")

        # Ensure file has .csv extension
        if not file_path.lower().endswith(".csv"):
            logging.error("Unsupported file format: %s", file_path)
            raise ValueError(f"Unsupported file format: {file_path}. Only CSV files are allowed.")
        encoding, delimiter = detect_encoding_and_delimiter(file_path)
        spark = get_spark_session()

        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("encoding", encoding) \
            .option("delimiter", delimiter) \
            .csv(file_path)

        logging.info("Encoding found: %s", encoding)
        logging.info("Delimiter found: %s", delimiter)
        logging.info("Data loaded successfully.")
        df.cache()
        logging.info("Row count: %s", df.count())  # Forces Spark to execute
        return df, spark  # Return both DataFrame and Spark session
    except Exception as error:
        logging.error("Error loading data: %s", str(error))
        raise

# Run as standalone module
if __name__ == "__main__":
    FILE_PATH = "/home/akshay/Iowa_Liquor_Sales.csv"

    # Debug file existence before processing
    if not os.path.exists(FILE_PATH):
        logging.error("File not found: %s", FILE_PATH)
        raise FileNotFoundError(f"File not found: {FILE_PATH}")

    DATA_DF, SPARK_SESSION = load_data(FILE_PATH)
    DATA_DF.show(5)
