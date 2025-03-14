from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import great_expectations as ge

def validate_data(df: DataFrame, bad_records_path: str):
    """Performs data quality checks using Great Expectations on PySpark DataFrame and returns only good records."""
    
    print(type(df))  # Debugging step to ensure df is a valid PySpark DataFrame
    
    expected_columns = ["Id", "Title", "review/score", "Price", "review/time", "review/helpfulness"]
    df = df.select(*[col for col in expected_columns if col in df.columns])  # Ensure only required columns are present
    
    df_ge = ge.dataset.SparkDFDataset(df)  # Use Great Expectations built-in PySpark validation
    
    # Define validation checks
    validation_checks = {
        "Id": df_ge.expect_column_values_to_not_be_null("Id"),
        "Title": df_ge.expect_column_values_to_not_be_null("Title"),
        "review/score": df_ge.expect_column_values_to_not_be_null("review/score"),
        "Price": df_ge.expect_column_values_to_be_between("Price", min_value=0),
        "Id uniqueness": df_ge.expect_column_values_to_be_unique("Id"),
        "review/time format": df_ge.expect_column_values_to_match_regex("review/time", "\\d+"),
        "review/helpfulness format": df_ge.expect_column_values_to_match_regex("review/helpfulness", "\\d+/\\d+")
    }
    
    # Filter valid records based on failed checks
    failed_checks = [col for col, result in validation_checks.items() if not result["success"]]
    if failed_checks:
        print(f"Validation checks failed for columns: {failed_checks}")
    
    df = df.withColumn("is_valid", (
        col("Id").isNotNull() &
        col("Title").isNotNull() &
        col("review/score").isNotNull() &
        (col("Price") >= 0) &
        col("review/time").rlike("\\d+") &
        col("review/helpfulness").rlike("\\d+/\\d+")
    ))
    
    good_records_df = df.filter(col("is_valid")).drop("is_valid")
    bad_records_df = df.filter(~col("is_valid")).drop("is_valid")
    
    # Store bad records in Parquet
    if bad_records_df.count() > 0:
        print(f"Bad records count: {bad_records_df.count()}")
        bad_records_df.show(truncate=False)  # Print a sample of bad records
        bad_records_df.write.mode("overwrite").parquet(bad_records_path)
    
    return good_records_df

if __name__ == "__main__":
    from load_data import load_data
    file_path = "/home/writv/pyspark_etl/dataset/Books_rating.csv"  # Update this
    df = load_data(file_path)
    validated_df = validate_data(df, "/home/writv/pyspark_etl/bad_data/to/validation_results.parquet")
    print("Data quality checks completed! Only good records returned.")
