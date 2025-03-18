import logging
from pyspark.sql import DataFrame

def write_to_mysql(df: DataFrame, url, dbtable, user, password):
    """Writes a PySpark DataFrame to a MySQL database with exception handling and logging."""
    try:
        logging.info(f"Starting data write to MySQL table: {dbtable}")
        df.write.format("jdbc") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("url", url) \
            .option("dbtable", dbtable) \
            .option("user", user) \
            .option("password", password) \
            .save()
        logging.info("Data successfully written to MySQL")
    except Exception as e:
        logging.error(f"Error writing data to MySQL: {e}")
        raise