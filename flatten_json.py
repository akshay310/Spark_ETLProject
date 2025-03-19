import re
import logging
from pyspark.sql import DataFrame
from typing import Dict
from pyspark.sql.functions import col, explode_outer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def rename_dataframe_cols(df: DataFrame, col_names: Dict[str, str]) -> DataFrame:
    """
    Rename all columns in dataframe
    """
    try:
        return df.select(*[col(col_name).alias(col_names.get(col_name, col_name)) for col_name in df.columns])
    except Exception as e:
        logging.error(f"Error renaming columns: {e}")
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

def flatten_json_df(df_arg: DataFrame, index: int = 1) -> DataFrame:
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
            data_type = str(field.dataType)
            column_name = field.name
            first_10_chars = data_type[0:10]
    
            if first_10_chars == 'ArrayType(':
                df_temp = df.withColumn(column_name, explode_outer(col(column_name)))
                return flatten_json_df(df_temp, index + 1)
            
            elif first_10_chars == 'StructType':
                current_col = column_name
                append_str = current_col
                data_type_str = str(df.schema[current_col].dataType)

                df_temp = df.withColumnRenamed(column_name, column_name + "#1") if column_name in data_type_str else df
                current_col = current_col + "#1" if column_name in data_type_str else current_col
                
                df_before_expanding = df_temp.select(f"{current_col}.*")
                newly_gen_cols = df_before_expanding.columns
                
                begin_index = append_str.rfind('*')
                end_index = len(append_str)
                level = append_str[begin_index + 1: end_index]
                next_level = int(level) + 1
                
                custom_cols = dict((field, f"{append_str}->{field}*{next_level}") for field in newly_gen_cols)
                df_temp2 = df_temp.select("*", f"{current_col}.*").drop(current_col)
                df_temp3 = df_temp2.transform(lambda df_x: rename_dataframe_cols(df_x, custom_cols))
                return flatten_json_df(df_temp3, index + 1)
        
        logging.info("Flattening complete.")
        return df
    except Exception as e:
        logging.error(f"Error flattening JSON DataFrame: {e}")
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