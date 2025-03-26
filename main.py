'''
Combines the reading , segregating the records and writting
'''
import logging
from config_read import load_config  # Import config loader
from parquet_read import create_spark_session, read_parquet_data
from quality_checks import quality_checks
from mssql_write import write_to_mssql

logging.basicConfig(level=logging.INFO,
                     format="%(asctime)s - %(levelname)s - %(message)s",
                     handlers = [
                        logging.FileHandler("app.log"),
                        logging.StreamHandler()
                     ])
logger = logging.getLogger(__name__)
def main():
    '''
    function creates a pipeline creates the sesssion reads the data segregates the data
    and writes the good records into mssql
    '''

    spark_session = create_spark_session()
    try:
        config_data = load_config()

        df_parquet = read_parquet_data(spark_session, config_data)

        good_data,bad_data = quality_checks(df_parquet, config_data)

        logger.info("Bad data saved separately.")
        logger.info("Bad data count: %d", bad_data.count())


        if good_data.count() > 0:
            write_to_mssql(good_data, config_data)
        else:
            logger.warning("No good data to write.")

    except Exception as e:
        logger.exception("Failed to create pipeline: %s", e)
    finally:
        spark_session.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()
