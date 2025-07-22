# main.py

from src.etl.csv_data_loader import load_csv
from src.etl.data_preprocess import preprocess_sensor_data
from src.etl.kpi_calculator import calculate_kpi
from src.etl.db_saver import save_to_sqlite
from src.etl.visualise_kpi_data import data_visualise
from src.etl.config import RAW_DATA_PATH, DB_PATH
from src.main.logger_config import setup_logger

# Setting up a module-specific logger
logger = setup_logger(__name__)

def main():
    df_raw = None
    preprocessed_df = None
    kpi_df = None

    try:
        logger.info("Step 1: Loading raw IoT sensor data...")
        df_raw = load_csv(RAW_DATA_PATH)
        if df_raw is None or df_raw.empty:
            logger.error("No data loaded. Exiting...")
            return
        logger.info(f"Preview of loaded Data:{df_raw.head()}")
    except Exception:
        logger.exception("ETL pipeline failed due to an unexpected loading error...")
        return

    try:
        logger.info("Step 2: Preprocessing sensor data...")
        preprocess_df = preprocess_sensor_data(df_raw)
        if preprocess_df is None or preprocess_df.empty:
            logger.error("No data preprocessed. Exiting...")
            return
        logger.info(f"Total rows of preprocessed Data:{len(preprocess_df)}")
        logger.info(f"Preview of preprocessed Data:{preprocess_df.head()}")
    except Exception:
        logger.exception("ETL pipeline failed due to an unexpected preprocessing error...")
        return

    try:
        logger.info("Step 3: Calculating KPIs...")
        kpi_df = calculate_kpi(preprocess_df)
        if kpi_df is None or kpi_df.empty:
            logger.error("No KPI data. Exiting...")
            return
        logger.info(f"KPIs: {kpi_df}")
    except Exception:
        logger.exception("ETL pipeline failed due to an unexpected error in calculation of KPIs...")
        return

    try:
        logger.info("Step 4: Saving processed data and KPIs to SQLite...")
        save_to_sqlite(preprocess_df, kpi_df, DB_PATH)
    except Exception:
        logger.exception("ETL pipeline failed due to an unexpected error in saving of cleaned data...")
        return

    try:
        logger.info("Step 5: Generating visualisations...")
        data_visualise(DB_PATH)
    except Exception:
        logger.exception("ETL pipeline failed due to an unexpected error in generating visualisations...")
        return

    logger.info("ETL pipeline executed successfully.")

if __name__ == "__main__":
    main()




