'''
Does the quality checks to the dataset
'''
import logging
from great_expectations.dataset import SparkDFDataset


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
        ge_df = SparkDFDataset(df)

        # Define Expectations (Quality Checks)
        expectations = {
            "trip_distance_invalid": 
            ge_df.expect_column_values_to_be_between("trip_distance", min_value=0.05, strict_min=True),
            "fare_amount_invalid": 
            ge_df.expect_column_values_to_be_between("fare_amount", min_value=2, strict_min=True),
            "tip_amount_invalid":
            ge_df.expect_column_values_to_be_between("tip_amount", min_value=0, max_value=df.select("fare_amount").first()[0] * 0.5),
            "trip_duration_invalid": 
            ge_df.expect_column_values_to_be_between("trip_duration", min_value=30, max_value=14400),
            "rate_code_invalid": 
            ge_df.expect_column_values_to_be_in_set("rate_code", [1, 2, 3, 4, 5, 6]),
            "store_and_fwd_flag_invalid": 
            ge_df.expect_column_values_to_be_in_set("store_and_fwd_flag", ["Y", "N"]),
            "payment_type_invalid": 
            ge_df.expect_column_values_to_be_in_set("payment_type", [1, 2, 3, 4, 6]),
        }

        # Identify failing records
        for key, expectation in expectations.items():
            if not expectation["success"]:
                logger.warning("%s : %d failed records",key,expectation['result']['unexpected_count'])
        # Filter bad records (any row failing an expectation)
        bad_data = df.filter(
            (df["trip_distance"] <= 0.05) |
            (df["fare_amount"] < 2) |
            (df["tip_amount"] < 0) | (df["tip_amount"] > df["fare_amount"] * 0.5) |
            (df["trip_duration"] < 30) | (df["trip_duration"] > 14400) |
            (~df["rate_code"].isin([1, 2, 3, 4, 5, 6])) |
            (~df["store_and_fwd_flag"].isin(["Y", "N"])) |
            (~df["payment_type"].isin([1, 2, 3, 4, 6]))
        )
        bad_count = bad_data.count()
        good_data = df.exceptAll(bad_data)
        good_count = good_data.count()

        # Logging results
        logger.info("Total records: %d",df.count())
        logger.info("Good records: %d", good_count)
        logger.info("Bad records: %d",bad_count)
        return good_data, bad_data
    except Exception as e:
        logger.exception("Error occurred during quality checks: %s", e)
        raise
