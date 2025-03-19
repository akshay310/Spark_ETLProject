'''
Does the quality checks to the dataset
'''
import logging
from great_expectations.dataset import SparkDFDataset
from pyspark.sql.functions import col, monotonically_increasing_id, when


# Configure logging
logger = logging.getLogger(__name__)

def quality_checks(df):
    """
    Perform data quality checks using Great Expectations and separate good/bad data.

    Parameters:
        df (DataFrame): The input Spark DataFrame.

    Returns:
        tuple: (good_data, bad_data)
    """
    try:
        logger.info("Starting data quality checks using Great Expectations...")
        df= df.withColumn("row_index", monotonically_increasing_id())
        ge_df = SparkDFDataset(df)

        # Define Expectations (Quality Checks)
        expectations = {
            "trip_distance": 
            ge_df.expect_column_values_to_be_between("trip_distance", min_value=0.05, strict_min=True),
            "fare_amount": 
            ge_df.expect_column_values_to_be_between("fare_amount", min_value=2, strict_min=True),
            "tip_amount":
            ge_df.expect_column_values_to_be_between("tip_amount", min_value=0, max_value=df.select("fare_amount").first()[0] * 0.5),
            "trip_duration": 
            ge_df.expect_column_values_to_be_between("trip_duration", min_value=30, max_value=14400),
            "rate_code": 
            ge_df.expect_column_values_to_be_in_set("rate_code", [1, 2, 3, 4, 5, 6]),
            "store_and_fwd_flag": 
            ge_df.expect_column_values_to_be_in_set("store_and_fwd_flag", ["Y", "N"]),
            "payment_type": 
            ge_df.expect_column_values_to_be_in_set("payment_type", [1, 2, 3, 4, 6]),
        }

        # Extract validation failures
        failed_conditions = []
        for key, result in expectations.items():
            if not result["success"]:
                failed_conditions.append((key, result["result"]["partial_unexpected_list"]))

        # Creating a failure flag column
        validation_status = df.withColumn("validation_status", when(col("row_index").isNotNull(), "PASS"))

        for col_name, failed_values in failed_conditions:
            validation_status = validation_status.withColumn(
                "validation_status",
                when((col(col_name).isin(failed_values)) | (col("validation_status") == "FAIL"), "FAIL").otherwise("PASS")
            )


        # Segregate the dataset into "pass" ancdd "fail"
        good_records = validation_status.filter(col("validation_status") == "PASS").drop("validation_status")
        bad_records = validation_status.filter(col("validation_status") == "FAIL").drop("validation_status")

        good_count = good_records.count()
        bad_count = bad_records.count()
        

        # Logging results
        logger.info("Total records: %d",df.count())
        logger.info("Good records: %d", good_count)
        logger.info("Bad records: %d",bad_count)
        return good_records, bad_records
    except Exception as e:
        logger.exception("Error occurred during quality checks: %s", e)
        raise
