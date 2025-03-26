import logging
from json_read import read_json_data, start_spark
from flatten_json import flatten_json_df, clean_column_names
from data_quality_check import validate_data_quality
from mysql_write import write_to_mysql
from read_config import get_input_file, get_bad_file, get_checks
from write_bad_records import write_parquet

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    config_file = "config.json"  # Configuration file path
    
    try:
        # Start a Spark session
        spark = start_spark("DQ")
    except Exception as e:
        logging.error("Failed to start Spark session: %s", e)
        return
    
    try:
        # Read input JSON data into a Spark DataFrame
        input_json_df = read_json_data(spark, get_input_file(config_file))
    except Exception as e:
        logging.error("Error reading JSON data: %s", e)
        return
    
    try:
        # Flatten nested JSON structures
        flattened_df = flatten_json_df(input_json_df)
        # Clean column names to remove unwanted characters
        cleaned_flattened_df = clean_column_names(flattened_df)
    except Exception as e:
        logging.error("Error processing JSON data: %s", e)
        return
    
    try:
        # Retrieve data quality checks from configuration
        checks = get_checks(config_file)
        # Validate data quality, separating good and bad records
        good_df, bad_df = validate_data_quality(cleaned_flattened_df, checks)
    except Exception as e:
        logging.error("Error in data quality validation: %s", e)
        return
    
    try:
        # Write bad records to a Parquet file
        write_parquet(bad_df, get_bad_file(config_file))
    except Exception as e:
        logging.error("Error writing bad records to Parquet: %s", e)
    
    try:
        # Define the target MySQL table
        db_table = "city_inspections"
        # Write good quality data to MySQL database
        write_to_mysql(good_df, db_table)
    except Exception as e:
        logging.error("Error writing to MySQL: %s", e)
    
if __name__ == "__main__":
    main()
