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
    try:
        spark = (SparkSession.builder 
            .appName(app_name)
            #.config("spark.sql.debug.maxToStringFields", 100)
            .config("spark.jars", "/home/reyona/pyproj/pyspark_proj_env/mysql-connector-j-9.2.0.jar")
            .master("local[*]")
            .getOrCreate()
        )
        logging.info("Spark session started successfully.")
        return spark
    except Exception as e:
        logging.error(f"Error initializing Spark session: {e}")
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
        logging.info(f"Successfully read JSON file: {file}")
        return df
    except FileNotFoundError:
        logging.error(f"File not found: {file}")
        raise
    except Exception as e:
        logging.error(f"Error reading JSON file {file}: {e}")
        raise