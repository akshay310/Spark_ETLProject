#load_data.py
from pyspark.sql import SparkSession
import logging
import csv
import chardet

def detect_encoding(file_path: str) -> str:
    """Detects file encoding using chardet."""
    with open(file_path, "rb") as f:
        raw_data = f.read(100000)  # Read first 100KB
        result = chardet.detect(raw_data)
    return result["encoding"] or "utf-8"

def detect_delimiter(file_path: str) -> str:
    """Detects the delimiter from the first line of the file."""
    with open(file_path, "r", encoding="utf-8") as f:
        first_line = f.readline()
        dialect = csv.Sniffer().sniff(first_line)
        return dialect.delimiter

def load_data(file_path: str):
    """Loads data from a CSV file with auto-detected encoding and delimiter."""
    logging.info(f"Detecting encoding for {file_path}...")
    encoding = detect_encoding(file_path)
    
    logging.info(f"Detecting delimiter for {file_path}...")
    delimiter = detect_delimiter(file_path)
    
    logging.info(f"Loading data with encoding={encoding} and delimiter={delimiter}")

    spark = SparkSession.builder \
        .appName("ETL-Load-Data") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "4g") \
        .config("spark.jars", "/opt/oracle/ojdbc11.jar") \
        .getOrCreate()
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", encoding) \
        .option("delimiter", delimiter) \
        .csv(file_path)

    logging.info(f"Loaded {df.count()} records from {file_path}.")
    return df
