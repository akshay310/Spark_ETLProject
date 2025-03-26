"""
Module to load CSV data into a PySpark DataFrame.
"""
import logging
import csv
import chardet
from pyspark.sql import SparkSession
from pyspark.errors import PySparkException

def detect_encoding(file_path: str, default_encoding="utf-8") -> str:
    """
    Detects file encoding using chardet.

    Args:
        file_path (str): Path to the file.
        default_encoding (str): Fallback encoding.

    Returns:
        str: Detected encoding.
    """
    try:
        with open(file_path, "rb") as f:
            raw_data = f.read(50000)  # Read only first 50KB to optimize performance
            result = chardet.detect(raw_data)
            encoding = result.get("encoding", default_encoding)
            logging.info(f"Detected encoding: {encoding}")
            return encoding
    except Exception as e:
        logging.error(f"Failed to detect encoding: {e}. Using default encoding: {default_encoding}")
        return default_encoding

def detect_delimiter(file_path: str) -> str:
    """
    Detects the delimiter from the first line of the file.

    Args:
        file_path (str): Path to the file.

    Returns:
        str: Detected delimiter.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            first_line = f.readline()
            dialect = csv.Sniffer().sniff(first_line)
            delimiter = dialect.delimiter
            logging.info(f"Detected delimiter: {delimiter}")
            return delimiter
    except Exception as e:
        logging.error(f"Failed to detect delimiter: {e}. Defaulting to ','.")
        return ","

def load_data(file_path: str, config):
    """
    Loads data from a CSV file with auto-detected encoding and delimiter.

    Args:
        file_path (str): Path to the CSV file.
        config (dict): Configuration dictionary.

    Returns:
        DataFrame: Loaded PySpark DataFrame.
    """
    try:
        logging.info(f"Starting data load for file: {file_path}")

        # Detect encoding and delimiter
        encoding = detect_encoding(file_path, config["etl_config"]["source"].get("encoding", "utf-8"))
        delimiter = detect_delimiter(file_path)

        # Reuse existing SparkSession if possible
        spark = SparkSession.builder \
            .appName("ETL-Load-Data") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "8g") \
            .config("spark.jars", "/opt/oracle/ojdbc11.jar") \
            .getOrCreate()

        # Load CSV into DataFrame
        df = spark.read \
            .option("header", config["etl_config"]["source"].get("header", True)) \
            .option("inferSchema", config["etl_config"]["source"].get("inferSchema", False)) \
            .option("encoding", encoding) \
            .option("delimiter", delimiter) \
            .csv(file_path)

        logging.info(f"Dataframe schema: {df.printSchema()}")
        # logging.info(f"Showing sample records:\n{df.limit(5).show(truncate=False)}")

        return df

    except PySparkException as e:
        logging.error(f"PySpark error during data load: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error in load_data: {e}")
        raise
