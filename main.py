from json_read import read_json_data, start_spark
from flatten_json import flatten_json_df, clean_column_names
from data_quality_check import validate_data_quality
from write_mysql import write_to_mysql

json_file = "city_inspections.json"
spark = start_spark("DQ")
input_json_df = read_json_data(spark,json_file)
flattened_df = flatten_json_df(input_json_df)
cleaned_flattened_df = clean_column_names(flattened_df)
# Check for non-null values in multiple columns
required_columns = ["business_name", "certificate_number", "id", "sector", "address_city", "address_street"]
allowed_values = {"result": ["Pass", "Fail", "No Violation Issued", "Violation Issued"]}
required_datatypes = {"address_zip" : "IntegerType"}
unique_values = ["id", "certificate_number","id_oid"]
good_df, bad_df = validate_data_quality(cleaned_flattened_df, required_columns, allowed_values, required_datatypes, unique_values)
bad_df.write.format("parquet").save("city_inspections_json_bad_records.parquet")
url_db="jdbc:mysql://localhost:3306/city"
db_table = "city_inspections"
user = "root"
password= ""
write_to_mysql(good_df,url_db,db_table,user,password)