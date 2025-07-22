# etl_pipeline_dag.py

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.etl.csv_data_loader import load_csv
from src.etl.data_preprocess import preprocess_sensor_data
from src.etl.kpi_calculator import calculate_kpi
from src.etl.db_saver import save_to_sqlite
from src.etl.visualise_kpi_data import data_visualise
from src.etl.config import RAW_DATA_PATH, DB_PATH
from src.main.logger_config import setup_logger

# Setting up a module-specific logger
logger = setup_logger(__name__)

default_args = {
    "owner": "airflow",
    "email": ["meenumary12@gmail.com"],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="etl_pipeline_dag",
    default_args=default_args,
    start_date=datetime(2025, 7, 22),
    schedule_interval="@daily",
    catchup=False,
    description="ETL pipeline",
    tags=["etl", "sensor"],
) as dag:

    def task_load_data(**context):
        try:
            df_raw = load_csv(RAW_DATA_PATH)
            context['ti'].xcom_push(key='raw_df', value=df_raw.to_json())
            logger.info("Loaded raw data and pushed to XCom.")
        except Exception as e:
            logger.error("Failed to load raw data: {}".format(e))
            raise

def task_preprocess_data(**context):
    try:
        raw_json = context['ti'].xcom_pull(key="raw_df")
        df_raw = pd.read_json(raw_json)
        preprocess_df = preprocess_sensor_data(df_raw)
        context['ti'].xcom_push(key='clean_df', value=preprocess_df.to_json())
        logger.info("Preprocessed raw data and pushed to XCom.")
    except Exception as e:
        logger.error("Failed to preprocess raw data: {}".format(e))
        raise

def task_calculate_kpi(**context):
    try:
        clean_json = context['ti'].xcom_pull(key="clean_df")
        preprocess_df = pd.read_json(clean_json)
        kpi_df = calculate_kpi(preprocess_df)
        context['ti'].xcom_push(key='kpi_df', value=kpi_df.to_json())
        logger.info("Calculated kpi dataframe and pushed to XCom.")
    except Exception as e:
        logger.error("Failed to calculate kpi: {}".format(e))
        raise

def task_save_to_db(**context):
    try:
        preprocess_df = pd.read_json(context['ti'].xcom_pull(key="clean_df"))
        kpi_df = pd.read_json(context['ti'].xcom_pull(key="kpi_df"))
        save_to_sqlite(preprocess_df, kpi_df, DB_PATH)
        logger.info("Saved cleaned data and pushed to XCom.")
    except Exception as e:
        logger.error("Failed to save cleaned data: {}".format(e))
        raise

def task_visualise(**context):
    try:
        data_visualise(DB_PATH)
        logger.info("Visualised data.")
    except Exception as e:
        logger.error("Failed to visualise data: {}".format(e))
        raise

t1 = PythonOperator(task_id='load_data', python_callable=task_load_data)
t2 = PythonOperator(task_id='preprocess_data', python_callable=task_preprocess_data)
t3 = PythonOperator(task_id='calculate_kpi', python_callable=task_calculate_kpi)
t4 = PythonOperator(task_id='save_to_db', python_callable=task_save_to_db)
t5 = PythonOperator(task_id='visualise_data', python_callable=task_visualise)

t1 >> t2 >> t3 >> t4 >> t5

