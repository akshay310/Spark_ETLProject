"""
Test module for load_data.py
"""
import unittest
import json
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pyspark.sql import SparkSession
from load_data import detect_encoding, detect_delimiter, load_data

class TestLoadData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up a Spark session for testing"""
        cls.spark = SparkSession.builder \
            .appName("Test-Load-Data") \
            .master("local[*]") \
            .getOrCreate()

        cls.test_file_path = "test_data.csv"
        cls.config = {
            "etl_config": {
                "source": {
                    "encoding": "utf-8",
                    "header": True,
                    "inferSchema": True
                }
            }
        }

        # Create a sample CSV file
        with open(cls.test_file_path, "w", encoding="utf-8") as f:
            f.write("name,age,city\nJohn,30,New York\nAlice,25,Los Angeles\n")

    @classmethod
    def tearDownClass(cls):
        """Clean up resources after tests"""
        cls.spark.stop()
        if os.path.exists(cls.test_file_path):
            os.remove(cls.test_file_path)

    def test_detect_encoding(self):
        """Test file encoding detection"""
        encoding = detect_encoding(self.test_file_path, self.config)
        self.assertIsInstance(encoding, str)

    def test_detect_delimiter(self):
        """Test delimiter detection"""
        delimiter = detect_delimiter(self.test_file_path)
        self.assertEqual(delimiter, ",")

    def test_load_data(self):
        """Test loading data into a PySpark DataFrame"""
        df = load_data(self.test_file_path, self.config)
        self.assertEqual(df.count(), 2)  # Two data rows
        self.assertEqual(len(df.columns), 3)  # Three columns

if __name__ == "__main__":
    unittest.main()
