# File: test_csv_loader.py

import pandas as pd

from etl.csv_loader import load_csv
from etl.config import RAW_DATA_PATH

def test_load_csv():
    raw_df = load_csv(RAW_DATA_PATH)
    assert isinstance(raw_df, pd.DataFrame)
    assert not raw_df.empty
    assert "temperature" in raw_df.columns

if __name__ == "__main__":
    test_load_csv()
