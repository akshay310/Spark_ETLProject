import pyspark
import re
from typing import Final, Dict, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, lit, explode_outer
from pyspark.sql.types import StructType, ArrayType, NumericType, DateType, StringType
import great_expectations as ge
from great_expectations.dataset import SparkDFDataset

def start_spark(app_name="DQ"):
    """
    Initializes and returns a Spark session.
    
    :param app_name: Name of the Spark application
    :return: SparkSess-rion object
    """
    spark = (SparkSession.builder 
        .appName(app_name)
        #.config("spark.sql.debug.maxToStringFields", 100)
        .config("spark.jars", "/home/reyona/pyproj/pyspark_proj_env/mysql-connector-j-9.2.0.jar")
        .master("local[*]")
        .getOrCreate()
    )
    
    return spark

def read_json_data(spark,file ):
    return spark.read.format("json").option("inferSchema",True).load(file)

def rename_dataframe_cols(df: DataFrame, col_names: Dict[str, str]) -> DataFrame:
    """
    Rename all columns in dataframe
    """
    return df.select(*[col(col_name).alias(col_names.get(col_name, col_name)) for col_name in df.columns])

def update_column_names(df: DataFrame, index: int) -> DataFrame:
    df_temp = df
    all_cols = df_temp.columns
    new_cols = dict((column, f"{column}*{index}") for column in all_cols)
    df_temp = df_temp.transform(lambda df_x: rename_dataframe_cols(df_x, new_cols))

    return df_temp

def flatten_json(df_arg: DataFrame, index: int = 1) -> DataFrame:
    """
    Flatten Json in a spark dataframe using recursion
    """
	# Update all column names with index 1
    df = update_column_names(df_arg, index) if index == 1 else df_arg

	# Get all field names fron the dataframe
    fields = df.schema.fields

	# For all columns in the dataframe
    for field in fields:
        data_type = str(field.dataType)
        column_name = field.name

        first_10_chars = data_type[0:10]
	
        # If it is an Array column
        if first_10_chars == 'ArrayType(':
            # Explode Array column
            df_temp = df.withColumn(column_name, explode_outer(col(column_name)))
            return flatten_json(df_temp, index + 1)

        # If it is a json object
        elif first_10_chars == 'StructType':
            current_col = column_name
            
            append_str = current_col

            # Get data type of current column
            data_type_str = str(df.schema[current_col].dataType)

            # Change the column name if the current column name exists in the data type string
            df_temp = df.withColumnRenamed(column_name, column_name + "#1") \
                if column_name in data_type_str else df
            current_col = current_col + "#1" if column_name in data_type_str else current_col

            # Expand struct column values
            df_before_expanding = df_temp.select(f"{current_col}.*")
            newly_gen_cols = df_before_expanding.columns

            # Find next level value for the column
            begin_index = append_str.rfind('*')
            end_index = len(append_str)
            level = append_str[begin_index + 1: end_index]
            next_level = int(level) + 1

            # Update column names with new level
            custom_cols = dict((field, f"{append_str}->{field}*{next_level}") for field in newly_gen_cols)
            df_temp2 = df_temp.select("*", f"{current_col}.*").drop(current_col)
            df_temp3 = df_temp2.transform(lambda df_x: rename_dataframe_cols(df_x, custom_cols))
            return flatten_json(df_temp3, index + 1)

    return df

def clean_column_names(df: DataFrame) -> DataFrame:
    def clean(name: str) -> str:
        name = re.sub(r'[^a-zA-Z_]', '_', name)  # Replace non-alphabetic characters with underscores
        name = re.sub(r'_+', '_', name)  # Remove consecutive underscores
        name = name.strip('_')  # Remove leading and trailing underscores
        return name
    
    new_columns = [clean(col) for col in df.columns]
    return df.toDF(*new_columns)

def get_date_columns(df):
    """Returns a list of date column names in a PySpark DataFrame."""
    return [field.name for field in df.schema.fields if isinstance(field.dataType, DateType)]

def validate_data_quality(spark_df):
    # Wrap the PySpark DataFrame with Great Expectations
    df_ge = SparkDFDataset(spark_df)

    date_regex_patterns = [
        r"\d{4}-\d{2}-\d{2}",  # YYYY-MM-DD
        r"\d{2}/\d{2}/\d{4}",  # MM/DD/YYYY
        r"\d{2}-\d{2}-\d{4}",  # DD-MM-YYYY
        r"\w{3} \d{1,2} \d{4}"  # Mon DD YYYY
    ]
    # Define expectations
    expectations = []
    
    # Check for non-null values in multiple columns
    required_columns = ["business_name", "certificate_number", "id", "sector", "address_city", "address_street"]
    for column in required_columns:
        expectations.append(df_ge.expect_column_values_to_not_be_null(column))
    
    # Validate date format
    date_columns = get_date_columns(spark_df)
    for date_col in date_columns:
        expectations.append(df_ge.expect_column_values_to_match_regex(date_col, date_regex_patterns))
    
    # Check values in a specific set
    expectations.append(df_ge.expect_column_values_to_be_in_set("result", ["Pass", "Fail", "No Violation Issued", "Violation Issued"]))
    
    # Validate ZIP code as an integer
    expectations.append(df_ge.expect_column_values_to_be_of_type("address_zip", "IntegerType"))
    
    # Check for unique values
    expectations.append(df_ge.expect_column_values_to_be_unique("id"))
    expectations.append(df_ge.expect_column_values_to_be_unique("certificate_number"))
    
    # Collect failed records
    failed_conditions = []
    for expectation in expectations:
        if not expectation["success"]:
            failed_conditions.append(expectation["expectation_config"]["kwargs"]["column"])
    
    # Separate good and bad records
    if failed_conditions:
        bad_records = spark_df.filter(
            col(failed_conditions[0]).isNull() | (col(failed_conditions[0]) == "")
        )
        for col_name in failed_conditions[1:]:
            bad_records = bad_records.union(spark_df.filter(
                col(col_name).isNull() | (col(col_name) == "")
            ))
        
        good_records = spark_df.subtract(bad_records)
    else:
        good_records = spark_df
        bad_records = spark_df.limit(0)  # Empty DataFrame
    
    return good_records, bad_records

# Example usage:
if __name__ == "__main__":
    json_file = "city_inspections.json"
    spark = start_spark("DQ")
    input_json_df = read_json_data(spark,json_file)
    #input_json_df = input_json_df.drop("_id")
    print(input_json_df.show())
    print(input_json_df.count())
    print(input_json_df.printSchema())

    flattened_df = flatten_json(input_json_df)
    flattened_df = clean_column_names(flattened_df)
    print(flattened_df.show())
    print(flattened_df.count())
    print(flattened_df.printSchema())

    good, bad = validate_data_quality(flattened_df)
    print(good.count())
    print(bad.count())

    bad.write.format("parquet").save("city_inspections_json_bad_records.parquet")
    
    # Write to MySQL Table
    good.write \
            .format("jdbc") \
            .option("driver","com.mysql.cj.jdbc.Driver") \
            .option("url", "jdbc:mysql://localhost:3306/city") \
            .option("dbtable", "city_inspections") \
            .option("user", "root") \
            .option("password", "") \
            .save() 

    spark.stop()  # Stop the session when done