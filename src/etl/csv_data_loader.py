# csv_data_loader.py

import pandas as pd
from typing import Union
from pathlib import Path

from src.main.logger_config import setup_logger

# Setting up a module-specific logger
logger = setup_logger(__name__)

def load_csv(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    Loads IoT sensor data from a csv file into a pandas dataframe.

    Args:
        file_path Union[str, Path]: Path to the csv file.

    Returns:
        pd.DataFrame: A pandas dataframe containing the loaded sensor data with parsed timestamps.

    Raises:
        FileNotFoundError: If the csv file does not exist.
        pd.errors.ParserError: Error parsing the csv file.
        Exception: For any other unexpected errors.
    """
    input_file_path = Path(file_path)

    if not input_file_path.exists():
        logger.error(f"File {input_file_path.resolve()} does not exist.")
        raise FileNotFoundError(f"File {input_file_path.resolve()} does not exist.")
    try:
        df = pd.read_csv(input_file_path, parse_dates=["timestamp"])
        logger.info(f"--- Loaded {len(df)} rows from {input_file_path.name}.")
        return df
    except pd.errors.ParserError as pe:
        logger.error(f"Error parsing {input_file_path.name}: {pe}.")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading {input_file_path.name}: {e}.")
        raise
