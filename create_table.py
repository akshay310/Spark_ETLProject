"""
Module to create the Oracle table if it does not exist.
"""
import os
import logging
import cx_Oracle

def create_table_if_not_exists(config):
    """
    Checks if the target table exists in Oracle. If it does not,
    creates the table with an extended size for the Title column.
    """
    table_name = config["etl_config"]["target"]["table_name"]
    oracle_url = os.getenv("ORACLE_URL")
    oracle_user = os.getenv("ORACLE_USER")
    oracle_password = os.getenv("ORACLE_PASSWORD")
    
    connection = None
    cursor = None

    try:
        # Connect to Oracle. If ORACLE_URL is not resolving, check your TNS configuration.
        connection = cx_Oracle.connect(user=oracle_user, password=oracle_password, dsn=oracle_url)
        cursor = connection.cursor()
        
        # Check if the table exists (adjust the owner if needed)
        check_sql = f"SELECT COUNT(*) FROM all_tables WHERE table_name = '{table_name.upper()}' AND owner = 'SYSTEM'"
        cursor.execute(check_sql)
        result = cursor.fetchone()[0]
        
        if result == 0:
            logging.info("Table %s does not exist. Creating new table.", table_name)
            create_sql = f"""
            CREATE TABLE {table_name} (
                "Id" NUMBER,
                "Title" VARCHAR2(500),
                "review/score" NUMBER,
                "Price" NUMBER,
                "review/time" VARCHAR2(50),
                "review/helpfulness" VARCHAR2(50)
            )
            """
            cursor.execute(create_sql)
            connection.commit()
            logging.info("Table %s created successfully.", table_name)
        else:
            logging.info("Table %s already exists.", table_name)
    except cx_Oracle.DatabaseError as e:
        logging.error("Error while creating table: %s", str(e))
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
