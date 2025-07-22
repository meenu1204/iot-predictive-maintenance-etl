# config.py

import os
from pathlib import Path
from dotenv import load_dotenv

# Read .env file and load all its KEY=value pairs
load_dotenv()

# Get base project directory
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# File paths
RAW_DATA_PATH = BASE_DIR / os.getenv("RAW_DATA_PATH")
CLEAN_DATA_PATH = BASE_DIR / os.getenv("CLEAN_DATA_PATH")
KPI_PATH = BASE_DIR / os.getenv("KPI_PATH")
DB_PATH = BASE_DIR / os.getenv("DB_PATH")
OUTPUT_PLOT = BASE_DIR / os.getenv("OUTPUT_PLOT")

# Sensor validation thresholds as valid ranges for cleaning sensor data
SENSOR_THRESHOLDS= {
    "temperature": (50, 100),
    "vibration": (0.0, 1.0),
    "pressure": (1.0, 10.0),
    "rpm": (500, 2500),
    "voltage": (180, 300)
}
