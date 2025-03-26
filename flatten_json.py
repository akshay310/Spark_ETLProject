"""
Module: flatten_json  
================================  

This module provides utility functions for processing and transforming PySpark DataFrames,  
with a focus on handling nested JSON structures, renaming columns, and cleaning column names.  

### Functions:
- `rename_dataframe_cols(df, col_names)`: Renames specified columns in a PySpark DataFrame.  
- `update_column_names(df, index)`: Appends an index to column names to ensure uniqueness.  
- `flatten_json_df(df_arg, index)`: Recursively flattens nested JSON structures within a DataFrame.  
- `clean_column_names(df)`: Cleans column names by replacing special characters with underscores.  

### Features:
- Handles complex nested JSON structures with recursion.  
- Ensures unique column names for better data consistency.  
- Logs operations and errors for easier debugging.  

Dependencies:
- `pyspark.sql` for Spark DataFrame operations.  
- `logging` for logging process information and errors.  
- `re` for regex-based column name cleaning.  
"""
import re
import logging
from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, StructType
from pyspark.sql.functions import col, explode_outer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def rename_dataframe_cols(df: DataFrame, col_names: Dict[str, str]) -> DataFrame:
    """
    Rename all columns in dataframe
    """
    try:
        return df.select(*[col(col_name).
                           alias(col_names.get(col_name, col_name)) for col_name in df.columns])
    except Exception as e:
        logging.error("Error renaming columns: %s", e)
        raise

def update_column_names(df: DataFrame, index: int) -> DataFrame:
    """
    Appends an index to all column names to ensure uniqueness.

    :param df: The input Spark DataFrame.
    :param index: The index to append to column names.
    :return: A Spark DataFrame with updated column names.
    """
    df_temp = df
    all_cols = df_temp.columns
    new_cols = dict((column, f"{column}*{index}") for column in all_cols)
    df_temp = df_temp.transform(lambda df_x: rename_dataframe_cols(df_x, new_cols))
    return df_temp

def flatten_json_df(df_arg: DataFrame, index: int = 1):
    """
    Recursively flattens nested JSON structures within a Spark DataFrame.

    :param df_arg: The input Spark DataFrame containing nested JSON fields.
    :param index: The recursion depth, starting at 1.
    :return: A flattened Spark DataFrame.
    :raises Exception: If the flattening process fails.
    """
    try:
        logging.info("Flattening JSON DataFrame......")
        df = update_column_names(df_arg, index) if index == 1 else df_arg
        fields = df.schema.fields

        for field in fields:
            data_type = field.dataType
            column_name = field.name

            if isinstance(data_type, ArrayType):
                # Explodes arrays keeping null values
                df_temp = df.withColumn(column_name, explode_outer(col(column_name)))
                return flatten_json_df(df_temp, index + 1)
            
            elif isinstance(data_type, StructType):
                current_col = column_name
                append_str = current_col
                data_type_str = str(df.schema[current_col].dataType)
                # Renames struct fields in case of duplicate names
                df_temp = df.withColumnRenamed(column_name, \
                            column_name + "#1") if column_name in data_type_str else df

                current_col = current_col + "#1" if column_name in data_type_str else current_col
                df_before_expanding = df_temp.select(f"{current_col}.*")
                # Gets the columns inside a struct fields
                newly_gen_cols = df_before_expanding.columns
                begin_index = append_str.rfind('*')
                end_index = len(append_str)
                level = append_str[begin_index + 1: end_index]

                next_level = int(level) + 1
                custom_cols = dict((field, f"{append_str}->{field}*{next_level}")
                                   for field in newly_gen_cols)
                df_drop_struct = df_temp.select("*", f"{current_col}.*").drop(current_col)
                df_flattened = df_drop_struct.transform(lambda df_x: rename_dataframe_cols(df_x, custom_cols))
                return flatten_json_df(df_flattened, index + 1)
        logging.info("Flattening complete.")
        return df
    except Exception as e:
        logging.error("Error flattening JSON DataFrame: %s",e)
        raise

def clean_column_names(df: DataFrame) -> DataFrame:
    """
    Cleans column names by replacing special characters with underscores.
    """
    def clean(name: str) -> str:
        name = re.sub(r'[^a-zA-Z_]', '_', name)
        name = re.sub(r'_+', '_', name)
        name = name.strip('_')
        return name
    new_columns = [clean(col) for col in df.columns]
    logging.info("Column names cleaned.")
    return df.toDF(*new_columns)
