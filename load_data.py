"""
Module to load CSV data into a PySpark DataFrame.
"""

import logging
import csv
import chardet
from pyspark.sql import SparkSession



def detect_encoding(file_path: str,config) -> str:
    """Detects file encoding using chardet"""
    with open(file_path, "rb") as f:
        raw_data = f.read(100000)  # Read first 100KB
        result = chardet.detect(raw_data)
    return result["encoding"] or config["etl_config"]["source"]["encoding"]

def detect_delimiter(file_path: str) -> str:
    """Detects the delimiter from the first line of the file"""
    with open(file_path, "r", encoding="utf-8") as f:
        first_line = f.readline()
        dialect = csv.Sniffer().sniff(first_line)
        return dialect.delimiter

def load_data(file_path: str,config):
    """Loads data from a CSV file with auto-detected encoding and delimiter"""
    logging.info("Detecting encoding for %s...", file_path)
    encoding = detect_encoding(file_path,config)

    logging.info("Detecting delimiter for %s...", file_path)
    delimiter = detect_delimiter(file_path)

    logging.info("Loading data with encoding=%s and delimiter=%s", encoding, delimiter)

    spark = SparkSession.builder \
        .appName("ETL-Load-Data") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "4g") \
        .config("spark.jars", "/opt/oracle/ojdbc11.jar") \
        .getOrCreate()

    df = spark.read \
        .option("header", config["etl_config"]["source"]["header"]) \
        .option("inferSchema", config["etl_config"]["source"]["inferSchema"]) \
        .option("encoding", encoding) \
        .option("delimiter", delimiter) \
        .csv(file_path)

    logging.info("Loaded %d records from %s.", df.count(), file_path)
    return df
