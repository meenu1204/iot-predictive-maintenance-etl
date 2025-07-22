# data_preprocess.py

import  pandas as pd
from src.main.logger_config import setup_logger
from src.etl.config import SENSOR_THRESHOLDS

# Setting up a module-specific logger
logger = setup_logger(__name__)

def preprocess_sensor_data(df: pd.DataFrame) -> pd.DataFrame:
    """
        Preprocess raw sensor data loaded into Pandas DataFrame.
        - Handling missing values.
        - Removing unrealistic sensor values (outliers).

        Args:
            pd.DataFrame.

        Returns:
            pd.DataFrame: Preprocessed DataFrame.
        """

    initial_length = df.shape[0]
    preprocessed_df = df.dropna().copy()
    dropped_rows = initial_length - preprocessed_df.shape[0]
    logger.info(f"Dropped {dropped_rows} rows with missing values.")

    # Apply range filters from config thresholds
    for field, (min_val, max_val) in SENSOR_THRESHOLDS.items():
        if field in preprocessed_df.columns:
            pre_filter_length = len(preprocessed_df)
            preprocessed_df = preprocessed_df[preprocessed_df[field].between(min_val, max_val)]
            post_filter_length = pre_filter_length - len(preprocessed_df)
            logger.info(f"Filtered out {post_filter_length} rows  for [{min_val} , {max_val}] for '{field}'.")
        else:
            logger.warning(f"Field '{field}' is not found in dataframe. Skipping threshold check.")

    preprocessed_df = preprocessed_df.reset_index(drop=True)
    logger.info(f"Final preprocessed data reduced to: {len(preprocessed_df)}.")
    
    return preprocessed_df
