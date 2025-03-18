# PySpark ETL Pipeline

This project is a modular ETL (Extract, Transform, Load) pipeline built using PySpark. It processes CSV files, applies data quality checks using Great Expectations, separates valid and invalid records, and loads clean data into an Oracle database.

## Features
- Load large CSV files into a PySpark DataFrame
- Perform data quality checks using Great Expectations
- Store invalid records in Parquet format
- Load valid records into an Oracle database
- Modular architecture for easy customization
- Follows Ruff-compliant coding standards

## Prerequisites
- **Python 3.10+**
- **PySpark**
- **Great Expectations**
- **Oracle 21c XE** (or any Oracle database)
- **Oracle Client Libraries**
- **Docker** (optional for Oracle setup)

## Setup Instructions

1. **Clone the Repository**
   ```sh
   git clone https://github.com/akshay310/Spark_ETLProject/tree/oracleDB
   cd Spark_ETLProject
   ```

2. **Create a Virtual Environment**
   ```sh
   python3 -m venv etl_env
   source etl_env/bin/activate  # On Windows: etl_env\Scripts\activate
   ```

3. **Install Dependencies**
   ```sh
   pip install -r etl_requirements.txt
   ```

4. **Set Up Environment Variables**
   - Create a `.env` file in the root directory.
   - Add your Oracle database connection details:
     ```env
     ORACLE_USER=your_username
     ORACLE_PASSWORD=your_password
     ORACLE_DSN=your_dsn
     ```

5. **Run the ETL Pipeline**
   Simply execute the `main.py` script to process data end-to-end:
   ```sh
   python main.py
   ```

## Project Structure
```
├── bad_data/               # Stores invalid records
├── dataset/                # Stores input CSV files
├── etl_env/                # Virtual environment (optional)
├── .env                    # Environment variables (user-defined)
├── data_quality.py         # Data validation using Great Expectations
├── load_data.py            # Loads CSV files into PySpark DataFrame
├── save_data.py            # Saves processed data into storage
├── load_to_oracle.py       # Loads cleaned data into Oracle DB
├── main.py                 # Orchestrates the entire ETL pipeline
├── sample_datasetgenerator.py # Generates sample CSV data
├── etl_requirements.txt    # List of dependencies
├── sample.csv              # Sample input file
├── README.md               # Project documentation
```

## Additional Notes
- To test Oracle connectivity, run:
  ```sh
  python oracle_test.py
  ```
- To inspect bad records, check the `bad_data/` directory.
- Modify `data_quality.py` to customize validation rules.

---

🚀 Happy Coding!

