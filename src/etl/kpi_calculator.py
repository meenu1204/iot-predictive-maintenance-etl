# kpi_calculator.py

import pandas as pd
from src.main.logger_config import setup_logger

# Setting up a module-specific logger
logger = setup_logger(__name__)

VIBRATION_ALERT_THRESHOLD = 0.08
VOLTAGE_LOWER_BOUND = 210
VOLTAGE_UPPER_BOUND = 250

def calculate_kpi(preprocess_df: pd.DataFrame) -> pd.DataFrame:
    """
            Calculate KPIs (Key performance indicators) for each machine.
            KPIs include:
            - Average temperature
            - Average RPM
            - Pressure variation
            - Count of vibration alerts (> 0.08)
            - Count of voltage anomalies (< 210 or > 250)

            Args:
                Preprocessed_df(pd.DataFrame): Preprocessed Sensor data.

            Returns:
                pd.DataFrame: Dataframe containing KPIs grouped by machine.
            """

    logger.info(f"\n--- Calculating KPIs ---")

    kpi_df = preprocess_df.groupby("machine_id").agg(
        avg_temperature=("temperature", "mean"),
        avg_rpm=("rpm", "mean"),
        pressure_variation=("pressure", "std"),
        vibration_alerts=("vibration", lambda x: (x > VIBRATION_ALERT_THRESHOLD).sum()),
        voltage_anomalies = ("voltage", lambda x: ((x < VOLTAGE_LOWER_BOUND) | (x > VOLTAGE_UPPER_BOUND)).sum())
    ).reset_index()

    logger.info(f"Generated KPIs for {len(kpi_df)} machines")
    return kpi_df