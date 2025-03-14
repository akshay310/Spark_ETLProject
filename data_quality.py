from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
import great_expectations as ge

def validate_data(df: DataFrame):
    """Performs data quality checks using Great Expectations and returns good and bad records."""
    print(f"🔹 Records before validation: {df.count()}") 
    expected_columns = ["Id", "Title", "review/score", "Price", "review/time", "review/helpfulness"]
    df = df.select(*[c for c in expected_columns if c in df.columns])  # Ensure only required columns are present
    print(f"🔹 Records after selection: {df.count()}") 
    df_ge = ge.dataset.SparkDFDataset(df)

    validation_checks = {
        "Id": df_ge.expect_column_values_to_not_be_null("Id"),
        "Title": df_ge.expect_column_values_to_not_be_null("Title"),
        "review/score": df_ge.expect_column_values_to_not_be_null("review/score"),
        "Price": df_ge.expect_column_values_to_be_between("Price", min_value=0),
        "Id uniqueness": df_ge.expect_column_values_to_be_unique("Id"),
        "review/time format": df_ge.expect_column_values_to_match_regex("review/time", "\\d+"),
        "review/helpfulness format": df_ge.expect_column_values_to_match_regex("review/helpfulness", "\\d+/\\d+")
    }

    failed_checks = [col for col, result in validation_checks.items() if not result["success"]]
    if failed_checks:
        print(f"Validation checks failed for columns: {failed_checks}")

    df = df.withColumn("is_valid", when(
        col("Id").isNotNull() &
        col("Title").isNotNull() &
        col("review/score").isNotNull() &
        (col("Price") >= 0) &
        col("review/time").rlike("\\d+") &
        col("review/helpfulness").rlike("\\d+/\\d+"),
        True
    ).otherwise(False))

    print(f"🔹 Total valid records: {df.filter(col('is_valid') == True).count()}")
    print(f"❌ Total invalid records: {df.filter(col('is_valid') == False).count()}")

    good_records_df = df.filter(col("is_valid") == True).drop("is_valid")
    print(f"✅ After filtering good records: {good_records_df.count()}")
    bad_records_df = df.filter(col("is_valid") == False).drop("is_valid")
    print(f"🚨 After filtering bad records: {bad_records_df.count()}")
    
    return good_records_df, bad_records_df

if __name__ == "__main__":
    from load_data import load_data
    file_path = "/home/writv/pyspark_etl/dataset/Books_rating.csv"
    
    df = load_data(file_path)
    good_records_df, bad_records_df = validate_data(df)
    
    print("Data quality checks completed!")



# if __name__ == "__main__":
#     from load_data import load_data
#     file_path = "/home/writv/pyspark_etl/dataset/Books_rating.csv"  # Update this
#     df = load_data(file_path)
#     validated_df = validate_data(df, "/home/writv/pyspark_etl/bad_data/to/validation_results.parquet")
#     print("Data quality checks completed! Only good records returned.")
