"""
Configuration file for PostgreSQL connection.
"""
import sys
import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Read database connection details
DB_URL = os.getenv("DB_URL")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Ensure all required variables are loaded
if not all([DB_URL, DB_USER, DB_PASSWORD]):
    logging.error("Missing required environment variables. Check your .env file.")
    sys.exit(1)

DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

logging.info("Successfully loaded database configuration.")
