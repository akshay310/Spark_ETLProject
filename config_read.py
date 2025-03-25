'''
Module loads the configuration setting from the JSON
'''
import json
import logging

logger = logging.getLogger(__name__)

def load_config(config_path="config.json"):
    """
    Loads the configuration from a JSON file.

    Parameters:
        config_path (str): The path to the configuration file.

    Returns:
        dict: A dictionary containing the configuration.
    """
    try:
        with open(config_path, "r", encoding="UTF-8") as file:
            config = json.load(file)
        return config
    except Exception as e:
        logger.exception("Failed to load configuration file: %s", e)
        raise
