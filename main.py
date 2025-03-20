from json_read import read_json_data, start_spark
from flatten_json import flatten_json_df, clean_column_names
from data_quality_check import validate_data_quality
from load_to_mysql import write_to_mysql
from read_config import get_input_file, get_bad_file, get_checks
from write_bad_records import write_parquet

config_file = "config.json"
spark = start_spark("DQ")
input_json_df = read_json_data(spark,get_input_file(config_file))
flattened_df = flatten_json_df(input_json_df)
cleaned_flattened_df = clean_column_names(flattened_df)
checks= get_checks(config_file)
good_df, bad_df = validate_data_quality(cleaned_flattened_df, checks)
write_parquet(bad_df, get_bad_file(config_file))
db_table = "city_inspections"
write_to_mysql(good_df,db_table)
