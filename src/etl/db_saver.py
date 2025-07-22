# db_saver.py

import os
import sqlite3
import pandas as pd
from src.main.logger_config import setup_logger


# Setting up a module-specific logger
logger = setup_logger(__name__)

def save_to_sqlite(preprocess_df: pd.DataFrame, kpi_df: pd.DataFrame, db_path: str) -> None:
    """
            Save processed sensor data and calculated KPIs to a SQLite database.

            Args:
                preprocess_df (pd.DataFrame) : Preprocessed sensor data.
                kpi_df (pd.DataFrame) : Calculate KPI metrics per machine.
                db_path (str) : Path to the SQLite database file.
                """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    try:
        with sqlite3.connect(db_path) as conn:
            if preprocess_df.empty:
                logger.warning(f"Preprocessed data is empty. Skipping save to 'preprocessed_sensor_data' table.")
            else:
                preprocess_df.to_sql("preprocessed_sensor_data", conn, if_exists="replace", index=False)
            if kpi_df.empty:
                logger.warning(f"KPI data is empty. Skipping save to 'kpi_data' table.")
            else:
                kpi_df.to_sql("machine_kpis", conn, if_exists="replace", index=False)
        logger.info(f"Data successfully saved to SQLite database at: {db_path}...")
    except Exception as e:
        logger.error(f"Error occurred while saving data to SQLite database: {e}...")
        raise