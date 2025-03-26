"""
Configuration file for PostgreSQL connection.

This module:
- Loads database connection parameters (URL, user, password) from `config.json`.
- Validates required fields and initializes `DB_URL` and `DB_PROPERTIES`.
- Handles common errors such as missing file, JSON parsing, or missing config values.

Typical usage:
    from db_cred import DB_URL, DB_PROPERTIES
"""

import sys
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Path to config file
CONFIG_FILE = "config.json"

# Load database configuration from config.json
try:
    with open(CONFIG_FILE, "r", encoding="utf-8") as file:
        config = json.load(file)
        db_config = config.get("database", {})
except FileNotFoundError:
    logging.error("Config file not found: %s", CONFIG_FILE)
    sys.exit(1)
except json.JSONDecodeError:
    logging.error("Error decoding JSON in config file: %s", CONFIG_FILE)
    sys.exit(1)
except Exception as e:
    logging.exception("Unexpected error reading config file: %s", str(e))
    sys.exit(1)

# Read database connection details
DB_URL = db_config.get("db_url")
DB_USER = db_config.get("db_user")
DB_PASSWORD = db_config.get("db_password")

# Ensure all required variables are loaded
if not all([DB_URL, DB_USER, DB_PASSWORD]):
    logging.error("Missing required database configuration in config.json.")
    sys.exit(1)

# JDBC-compatible properties dictionary
DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

logging.info("✅ Successfully loaded database configuration from config.json.")
