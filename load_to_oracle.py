#load_to_oracle.py
import logging
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Oracle Database Connection Details
ORACLE_URL = "jdbc:oracle:thin:@localhost:1521/XEPDB1"
ORACLE_USER = "system"
ORACLE_PASSWORD = "nabakallolghosh"
ORACLE_DRIVER = "oracle.jdbc.OracleDriver"

def save_to_oracle(df: DataFrame, table_name: str) -> None:
    """
    Saves a cleaned and validated PySpark DataFrame to an Oracle database.
    
    Args:
        df (DataFrame): The DataFrame containing valid records.
        table_name (str): The target Oracle table name.
    """
    try:
        logging.info("Saving DataFrame to Oracle database...")
        
        df.write \
            .format("jdbc") \
            .option("url", ORACLE_URL) \
            .option("dbtable", table_name) \
            .option("user", ORACLE_USER) \
            .option("password", ORACLE_PASSWORD) \
            .option("driver", ORACLE_DRIVER) \
            .option("sessionInitStatement", "ALTER SESSION SET ISOLATION LEVEL READ COMMITTED")\
            .mode("append") \
            .save()
        
        logging.info("Data successfully saved to Oracle table: %s", table_name)
    except AnalysisException as e:
        logging.error("AnalysisException encountered: %s", str(e))
    except Exception as e:
        logging.error("Failed to save data to Oracle: %s", str(e))

'''import cx_Oracle
from pyspark.sql import DataFrame
import logging

def create_table_if_not_exists(connection, table_name: str, df: DataFrame):
    """
    Creates an Oracle table if it does not exist.

    Args:
        connection: Oracle connection.
        table_name (str): Name of the table.
        df (DataFrame): DataFrame containing the schema.
    """
    cursor = connection.cursor()
    columns = [f'"{col}" VARCHAR2(255)' for col in df.columns]
    create_table_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns)})'

    try:
        cursor.execute(create_table_sql)
    except cx_Oracle.DatabaseError:
        logging.warning(f"Table {table_name} already exists.")
    finally:
        cursor.close()

def save_to_oracle(df: DataFrame, table_name: str):
    """
    Inserts data into an Oracle table.

    Args:
        df (DataFrame): Cleaned DataFrame.
        table_name (str): Oracle table name.
    """
    dsn = cx_Oracle.makedsn("localhost", 1521, service_name="XEPDB1")
    connection = cx_Oracle.connect(user="system", password="nabakallolghosh", dsn=dsn)
    
    create_table_if_not_exists(connection, table_name, df)
    
    df.write.format("jdbc").option("url", f"jdbc:oracle:thin:@localhost:1521/XEPDB1") \
        .option("dbtable", table_name).option("user", "system") \
        .option("password", "password").mode("append").save()

    logging.info(f"Inserted {df.count()} records into {table_name}.")
'''