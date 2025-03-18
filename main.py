'''
Combines the reading , segregating the records and writting
'''
import logging
from load_parquet import create_spark_session, read_parquet_data
from quality_checks import quality_checks
from load_to_mssql import load_db_config, write_to_mssql

# Configure logging
logging.basicConfig(level=logging.INFO,
                     format="%(asctime)s - %(levelname)s - %(message)s",
                     handlers = [
                        logging.FileHandler("app.log"),
                        logging.StreamHandler()
                     ])
logger = logging.getLogger(__name__)

def main(file_path):
    '''
    function creates a pipeline creates the sesssion reads the data segregates the data
    and writes the good records into mssql
    
    Parameters:
    file_path(string): takes the path of dataset in the command line

    '''
    spark = create_spark_session()
    try:
        df = read_parquet_data(spark, file_path)

        good_data, bad_data = quality_checks(df)

        bad_data.write.mode("overwrite").parquet("bad_data")
        logger.info("Bad data saved separately.")

        if good_data.count() > 0:
            db_config = load_db_config('/home/vaishnavi/nyc_taxi_pyspark/nyc_taxi/config.json') 

            write_to_mssql(good_data, db_config)
        else:
            logger.warning("No good data to write.")

    except Exception as e:
        logger.exception("Failed to create pipeline: %s", e)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        logger.error("Usage: python main.py <input_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    main(input_path)
