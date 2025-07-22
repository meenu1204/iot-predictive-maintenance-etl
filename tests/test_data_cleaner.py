# File: test_data_cleaner.py

import pandas as pd

from src.etl.csv_data_loader import load_csv
from src.etl.data_preprocess import clean_sensor_data
from etl.config import RAW_DATA_PATH

def test_clean_sensor_data():
    raw_df = load_csv(RAW_DATA_PATH)
    cleaned_df = clean_sensor_data(raw_df)
    assert isinstance(cleaned_df, pd.DataFrame)
    assert "temperature" in cleaned_df.columns
    assert cleaned_df["temperature"].between(50,100).all()

if __name__ == "__main__":
    test_clean_sensor_data()