import cx_Oracle
from pyspark.sql import DataFrame
import re
from datetime import datetime

def sanitize_column_name(col_name: str) -> str:
    """Sanitizes column names by replacing special characters with underscores."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', col_name.upper())

def create_table_if_not_exists(connection, table_name: str, df: DataFrame):
    """Creates the table dynamically based on DataFrame columns if it does not exist."""
    cursor = connection.cursor()
    
    columns = []
    for field in df.schema.fields:
        col_name = sanitize_column_name(field.name)
        if field.dataType.simpleString() == "string":
            col_type = "VARCHAR2(255)"
        elif field.dataType.simpleString() == "int":
            col_type = "NUMBER"
        elif field.dataType.simpleString() == "double":
            col_type = "NUMBER(10,2)"
        elif field.dataType.simpleString() == "timestamp":
            col_type = "DATE"
        else:
            col_type = "VARCHAR2(255)"
        columns.append(f'"{col_name}" {col_type}')
    
    create_table_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns)})'
    
    try:
        cursor.execute(create_table_sql)
    except cx_Oracle.DatabaseError as e:
        print(f"⚠️ Error creating table: {str(e)}")
    finally:
        cursor.close()

def format_review_time(review_time):
    """Converts review_time to Oracle-compatible format or replaces invalid values."""
    try:
        timestamp = int(float(review_time))  # Ensure it's a proper numeric timestamp
        if timestamp < 0:
            return None  # Invalid timestamp
        return datetime.utcfromtimestamp(timestamp).strftime('%d-%b-%Y %H:%M:%S')
    except (ValueError, TypeError):
        return None  # Skip invalid dates

def clean_value(value, max_length=255):
    """Truncates string values and handles numeric conversion errors."""
    if isinstance(value, str):
        return value[:max_length]  # Truncate long text
    if isinstance(value, (int, float)):
        return value  # Keep numeric values as is
    return None  # Replace invalid values with NULL

def save_to_oracle(df: DataFrame, table_name: str, batch_size: int = 500):
    """Saves a PySpark DataFrame to an Oracle table using batch inserts."""
    dsn = cx_Oracle.makedsn("localhost", 1521, service_name="XEPDB1")
    connection = cx_Oracle.connect(user="system", password="nabakallolghosh", dsn=dsn)
    
    create_table_if_not_exists(connection, table_name, df)  # Ensure table exists with correct schema
    
    cursor = connection.cursor()
    
    sanitized_columns = [sanitize_column_name(col) for col in df.columns]
    column_names = ", ".join(f'"{col}"' for col in sanitized_columns)
    placeholders = ", ".join([f":{i+1}" for i in range(len(sanitized_columns))])
    insert_query = f"INSERT INTO \"{table_name}\" ({column_names}) VALUES ({placeholders})"
    
    records = []
    invalid_count = 0
    for row in df.collect():
        row_list = list(row)
        if 'review/time' in df.columns:
            idx = df.columns.index('review/time')
            formatted_time = format_review_time(row_list[idx])
            row_list[idx] = formatted_time if formatted_time else None  # Handle invalid timestamps
        
        row_list = [clean_value(val) for val in row_list]  # Clean all values
        records.append(tuple(row_list))
    
    try:
        for i in range(0, len(records), batch_size):
            cursor.executemany(insert_query, records[i:i+batch_size])
            connection.commit()
        print(f"✅ Successfully inserted {len(records)} records into {table_name}")
        print(f"🚨 Skipped {invalid_count} records due to invalid timestamps or values")
    except cx_Oracle.DatabaseError as e:
        print(f"❌ Error inserting records: {str(e)}")
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    from data_quality import validate_data
    from load_data import load_data
    
    file_path = "/home/writv/pyspark_etl/dataset/Books_rating.csv"
    df = load_data(file_path)
    good_records_df, bad_records_df = validate_data(df)
    
    save_to_oracle(good_records_df, "BOOKSRATINGS")
