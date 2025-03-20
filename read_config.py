"""
Module: data_processing

This module provides functions to process JSON configuration files, 
extract file paths, and retrieve data quality checks with exception handling.
"""

import json
import logging

def load_json_req(data_req_file):
    """Loads a JSON file and returns the parsed data.
    
    Args:
        data_req_file (str): Path to the JSON file.
    
    Returns:
        dict: Parsed JSON data.
    
    Raises:
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file is not a valid JSON.
    """
    try:
        with open(data_req_file, 'r') as data_file:
            return json.load(data_file)
    except FileNotFoundError as e:
        logging.error("JSON file not found: %s", data_req_file)
        raise e
    except json.JSONDecodeError as e:
        logging.error("Error decoding JSON file: %s", data_req_file)
        raise e

def get_input_file(data_req_file):
    """Extracts the input file path from the JSON configuration.
    
    Args:
        data_req_file (str): Path to the JSON file.
    
    Returns:
        str: Input file path.
    
    Raises:
        KeyError: If the required keys are missing.
    """
    try:
        data = load_json_req(data_req_file)
        return data["task"]["source"]["file_path"] + data["task"]["source"]["file_name"]
    except KeyError as e:
        logging.error("Missing key in JSON: %s", str(e))
        raise e

def get_bad_file(data_req_file):
    """Extracts the bad record file path from the JSON configuration.
    
    Args:
        data_req_file (str): Path to the JSON file.
    
    Returns:
        str: Bad record file path.
    
    Raises:
        KeyError: If the required keys are missing.
    """
    try:
        data = load_json_req(data_req_file)
        return data["task"]["target"]["bad_record_file_path"] + \
                data["task"]["target"]["bad_record_file_name"]
    except KeyError as e:
        logging.error("Missing key in JSON: %s", str(e))
        raise e

def get_checks(data_req_file):
    """Extracts data quality checks from the JSON configuration.
    
    Args:
        data_req_file (str): Path to the JSON file.
    
    Returns:
        dict: Data quality checks mapping.
    
    Raises:
        KeyError: If the required keys are missing.
    """
    try:
        data = load_json_req(data_req_file)
        checks = {}
        for i in data["task"]["data_quality"]:
            check_type = i["check"].split("_")[-1]
            if check_type == "set":
                values = [value.strip() for value in i["parameters"]["value_set"].split(",")]
                checks[i["check"]] = {i["parameters"]["column"]: values}
            elif check_type in ["unique", "null"]:
                checks[i["check"]] = [i["parameters"]["column"]]
            elif check_type == "type":
                checks[i["check"]] = {i["parameters"]["column"]: i["parameters"]["type"]}
        return checks
    except KeyError as e:
        logging.error("Missing key in JSON: %s", str(e))
        raise e
    