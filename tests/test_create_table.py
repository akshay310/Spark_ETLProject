import pytest
import logging
from unittest.mock import patch, MagicMock
import os 
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from create_table import create_table_if_not_exists  # Ensure correct import

@pytest.fixture
def mock_config():
    return {"etl_config": {"target": {"table_name": "test_table"}}}

def normalize_sql(sql):
    """Removes extra spaces and newlines for consistent SQL comparison."""
    import re
    return re.sub(r"\s+", " ", sql.strip())  # Replaces multiple spaces/newlines with a single space

@patch("cx_Oracle.connect")
def test_create_table_when_not_exists(mock_connect, mock_config):
    """Test that table is created if it does not exist."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    # Simulate that the table does NOT exist
    mock_cursor.fetchone.return_value = [0]

    with patch("logging.info") as mock_log:
        create_table_if_not_exists(mock_config)

    # Define expected SQL query
    expected_sql = """
        CREATE TABLE test_table (
            "Id" NUMBER,
            "Title" VARCHAR2(500),
            "review/score" NUMBER,
            "Price" NUMBER,
            "review/time" VARCHAR2(50),
            "review/helpfulness" VARCHAR2(50)
        )
    """

    # Check if the actual query matches expected (ignoring whitespace issues)
    assert any(
        normalize_sql(call_args[0][0]) == normalize_sql(expected_sql)
        for call_args in mock_cursor.execute.call_args_list
    ), "CREATE TABLE statement not found in executed queries"

    mock_log.assert_any_call("Table %s created successfully.", "test_table")

@patch("cx_Oracle.connect")
def test_table_already_exists(mock_connect, mock_config):
    """Test that no table creation happens if the table already exists."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    # Simulate that the table ALREADY exists
    mock_cursor.fetchone.return_value = [1]

    with patch("logging.info") as mock_log:
        create_table_if_not_exists(mock_config)

    # Ensure CREATE TABLE query was NOT executed
    for call_args in mock_cursor.execute.call_args_list:
        assert "CREATE TABLE" not in call_args[0][0], "Table creation should NOT have been executed"

    mock_log.assert_any_call("Table %s already exists.", "test_table")
