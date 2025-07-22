# visualise_kpi_data.py

import sqlite3
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from src.main.logger_config import setup_logger
from src.etl.config import OUTPUT_PLOT

# Setting up a module-specific logger
logger = setup_logger(__name__)

def save_plot(plot_df: pd.DataFrame, x:str, y:str, title:str, ylabel:str, output_plot_filename:str) -> None:
    """
        Generate and save a bar plot.
    """
    if y not in plot_df.columns or x not in plot_df.columns:
        logger.warning(f"Missing columns: {x} or {y}. Skipping plot '{output_plot_filename}'")
        return

    if plot_df[y].sum() == 0:
        logger.warning(f"Skipping plot '{output_plot_filename}' — all values in '{y}' are zero.")
        return
    else:
        logger.info(f"Generating plot '{output_plot_filename}' ...'")

    try:
        plt.figure(figsize=(8, 5))
        plt.bar(plot_df[x], plot_df[y])
        plt.title(title)
        plt.xlabel("Machine ID")
        plt.ylabel(ylabel)
        plt.tight_layout()

        output_dir = Path(OUTPUT_PLOT)
        output_dir.mkdir(parents=True, exist_ok=True)
        full_path = output_dir / output_plot_filename
        plt.savefig(full_path)
        logger.info(f"Saved plot '{full_path}'")
    except Exception as e:
        logger.exception(f"Failed to generate plot '{output_plot_filename}'")
        raise

def data_visualise(db_path:str) -> None:
    """
       Load KPI data from SQLite and generate visualizations

       Args:
           db_path (str): Path to the SQLite database containing KPI data.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            plot_df = pd.read_sql("SELECT * FROM machine_kpis", conn)

        save_plot(plot_df, "machine_id", "avg_temperature", "Average Temperature per Machine",
                  "Avg Temperature", "avg_temperature.png")
        save_plot(plot_df, "machine_id", "vibration_alerts", "Vibration Alerts per Machine", "Alert Count",
                  "vibration_alerts.png")
        save_plot(plot_df, "machine_id", "voltage_anomalies", "Voltage Anomalies per Machine", "Anomaly Count",
                  "voltage_anomalies.png")
        logger.info(f"Data visualisations generated successfully...")

    except Exception as e:
        logger.error(f"Failed to generate KPI visualisations: {e}...")
        raise