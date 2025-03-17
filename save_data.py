#save_data.py
from pyspark.sql import DataFrame

def save_data(good_records_df: DataFrame, bad_records_df: DataFrame, bad_records_path: str):
    """Saves bad records to Parquet and prepares good records for Oracle ingestion."""
    
    if bad_records_df.count() > 0:
        print(f"Bad records count: {bad_records_df.count()}")
        bad_records_df.write.mode("overwrite").parquet(bad_records_path)
        print(f"Bad records saved to {bad_records_path}")
    if good_records_df.count() > 0:
        print(f"Good records count: {good_records_df.count()}")

    print("Good records ready for Oracle ingestion.")
    return good_records_df

if __name__ == "__main__":
    from load_data import load_data
    from data_quality import validate_data

    file_path = "/home/writv/pyspark_etl/dataset/Books_rating.csv"
    bad_records_path = "/home/writv/pyspark_etl/bad_data/to/validation_results.parquet"

    df = load_data(file_path)
    good_records_df, bad_records_df = validate_data(df)
    
    good_records_df = save_data(good_records_df, bad_records_df, bad_records_path)
