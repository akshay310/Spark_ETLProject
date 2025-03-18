import pytest
from pyspark.sql import SparkSession
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from load_data import load_data
from data_quality import validate_and_clean_data
from save_data import save_data
from load_to_oracle import save_to_oracle



@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("TestETL").getOrCreate()

@pytest.fixture
def sample_csv(tmp_path):
    file_path = tmp_path / "test_data.csv"
    data = """Id,Title,review/score,Price,review/time,review/helpfulness
1,Book A,5,10,1620000000,5/5
2,Book B,4,15,1610000000,3/5
3,Book C,,20,1600000000,2/5
4,Book D,3,-5,1590000000,1/5
5,Book E,5,30,invalid_time,0/5
"""
    file_path.write_text(data)
    return str(file_path)

def test_load_data(spark, sample_csv):
    df = load_data(sample_csv)
    assert df is not None
    assert df.count() == 5
    assert set(df.columns) == {"Id", "Title", "review/score", "Price", "review/time", "review/helpfulness"}

def test_validate_and_clean_data(spark, sample_csv):
    df = load_data(sample_csv)
    good_records_df, bad_records_df = validate_and_clean_data(df)
    
    assert good_records_df.count() > 0
    assert bad_records_df.count() > 0
    assert "is_valid" not in good_records_df.columns

def test_save_data(spark, sample_csv, tmp_path):
    df = load_data(sample_csv)
    good_records_df, bad_records_df = validate_and_clean_data(df)
    bad_records_path = str(tmp_path / "bad_records.parquet")
    
    save_data(good_records_df, bad_records_df, bad_records_path)
    assert os.path.exists(bad_records_path) if bad_records_df.count() > 0 else True

def test_save_to_oracle(spark, sample_csv, monkeypatch):
    df = load_data(sample_csv)
    good_records_df, _ = validate_and_clean_data(df)
    
    monkeypatch.setenv("ORACLE_URL", "jdbc:oracle:thin:@localhost:1521/XEPDB1")
    monkeypatch.setenv("ORACLE_USER", "system")
    monkeypatch.setenv("ORACLE_PASSWORD", "password")
    monkeypatch.setenv("ORACLE_DRIVER", "oracle.jdbc.OracleDriver")
    
    try:
        save_to_oracle(good_records_df, "TEST_BOOKRATINGS")
    except Exception as e:
        pytest.fail(f"save_to_oracle failed: {e}")
