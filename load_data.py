"""
Module to load CSV data into a PySpark DataFrame.
"""
import logging
import csv
import chardet
import json
import os"""""
Module to load CSV data into a PySpark DataFrame with automatic delimiter and encoding detection.
"""

import logging
import csv
import os
import json
import chardet
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)

# Load configuration from config.json
with open("config.json", "r") as config_file:
    config = json.load(config_file)

FILE_PATH = config["task"]["source"]["file_path"] + config["task"]["source"]["file_name"]


def detect_encoding_and_delimiter(file_path, sample_size=10000):
    """
    Detects the file encoding and delimiter using chardet and csv.Sniffer.

    Args:
        file_path (str): Path to the CSV file.
        sample_size (int): Number of bytes to sample for encoding detection.

    Returns:
        Tuple[str, str]: A tuple containing detected encoding and delimiter.

    Raises:
        UnicodeDecodeError: If encoding detection fails.
        csv.Error: If delimiter detection fails.
    """
    with open(file_path, "rb") as file:
        raw_data = file.read(sample_size)
        encoding_info = chardet.detect(raw_data)
        encoding = encoding_info.get("encoding", "utf-8")

    with open(file_path, "r", encoding=encoding) as file:
        sample = file.readline()
        delimiter = csv.Sniffer().sniff(sample).delimiter

    logging.info("Detected Encoding: %s", encoding)
    logging.info("Detected Delimiter: %s", delimiter)

    return encoding, delimiter


def get_spark_session(app_name="ETL-Load-Data"):
    """
    Initializes and returns a SparkSession with custom configurations.

    Args:
        app_name (str, optional): Name of the Spark application. Defaults to "ETL-Load-Data".

    Returns:
        SparkSession: A configured SparkSession object.

    Raises:
        Exception: If SparkSession creation fails.
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
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully.")
        return spark
    except Exception as error:
        logging.error("Error creating Spark session: %s", str(error))
        raise


def load_data(file_path):
    """
    Loads a CSV file into a PySpark DataFrame using detected encoding and delimiter.

    Args:
        file_path (str): Absolute path to the input CSV file.

    Returns:
        Tuple[DataFrame, SparkSession]: A tuple containing the loaded DataFrame and SparkSession.

    Raises:
        FileNotFoundError: If the file doesn't exist.
        ValueError: If the file is empty or not a CSV.
        Exception: For any other errors during loading.
    """
    try:
        if not os.path.exists(file_path):
            logging.error("File not found: %s", file_path)
            raise FileNotFoundError(f"File not found: {file_path}")

        if os.stat(file_path).st_size == 0:
            logging.error("File is empty: %s", file_path)
            raise ValueError(f"File is empty: {file_path}")

        if not file_path.lower().endswith(".csv"):
            logging.error("Unsupported file format: %s", file_path)
            raise ValueError(f"Unsupported file format: {file_path}. Only CSV files are allowed.")
        
        encoding, delimiter = detect_encoding_and_delimiter(file_path)
        spark = get_spark_session()

        df = spark.read \
            .option("header", config["task"]["source"]["header"]) \
            .option("inferSchema", "true") \
            .option("encoding", encoding) \
            .option("delimiter", delimiter) \
            .csv(file_path)

        logging.info("Data loaded successfully.")
        df.cache()
        logging.info("Row count: %s", df.count())
        return df, spark
    except Exception as error:
        logging.error("Error loading data: %s", str(error))
        raise


if __name__ == "__main__":
    DATA_DF, SPARK_SESSION = load_data(FILE_PATH)
    DATA_DF.show(5)
from pyspark.sql import SparkSession
from pyspark.errors import PySparkException

logging.basicConfig(level=logging.INFO)

# Load configuration from config.json
with open("config.json", "r") as config_file:
    config = json.load(config_file)

FILE_PATH = config["etl_config"]["source"]["file_path"] + config["etl_config"]["source"]["file_name"]

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

def get_spark_session(app_name="ETL-Load-Data") -> SparkSession:
    """
    Initializes and returns a SparkSession with custom configurations.

    Args:
        app_name (str, optional): Name of the Spark application. Defaults to "ETL-Load-Data".

    Returns:
        SparkSession: A configured SparkSession object.
    """
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memoryOverhead", "1g") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.5") \
            .config("spark.jars", "/opt/oracle/ojdbc11.jar") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        raise

def load_data(file_path: str, config: dict):
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

        if not os.path.exists(file_path):
            logging.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")

        if os.stat(file_path).st_size == 0:
            logging.error(f"File is empty: {file_path}")
            raise ValueError(f"File is empty: {file_path}")

        encoding = detect_encoding(file_path, config["etl_config"]["source"].get("encoding", "utf-8"))
        delimiter = detect_delimiter(file_path)

        spark = get_spark_session()

        df = spark.read \
            .option("header", config["etl_config"]["source"].get("header", True)) \
            .option("inferSchema", config["etl_config"]["source"].get("inferSchema", False)) \
            .option("encoding", encoding) \
            .option("delimiter", delimiter) \
            .csv(file_path)

        logging.info("Data loaded successfully.")
        df.cache()
        logging.info(f"Row count: {df.count()}")
        return df
    except PySparkException as e:
        logging.error(f"PySpark error during data load: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error in load_data: {e}")
        raise

if __name__ == "__main__":
    DATA_DF = load_data(FILE_PATH, config)
    DATA_DF.show(5)
