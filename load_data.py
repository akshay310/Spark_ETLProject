#load_data.py
from pyspark.sql import SparkSession

def load_data(file_path):
    """Loads a book CSV file into a PySpark DataFrame.
    shuffle partitions 200 for better optimization"""
    spark = SparkSession.builder \
        .appName("ETL-Load-Data") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)
    print(f"🔹 Total records loaded: {df.count()}")
    return df

if __name__ == "__main__":
    file_path = "dataset/Books_rating.csv"  # Update this
    df = load_data(file_path)
    df.show(5)  # Display sample rows
