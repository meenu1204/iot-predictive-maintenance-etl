# IoT Predictive Maintenance ETL Pipeline

## Project Overview:
-------------------
A production-ready ETL pipeline that processes IoT sensor logs from machines that can help anticipate equipment failures.

## Pipeline Features:
--------------------
a. Load the raw IoT Sensor data
b. Clean, validate and transform sensor data
c. Calculate machine health KPIs
d. Store cleaned data + KPIs in SQLite
e. Visualise trends and anomalies using matplotlib
f. Visualise machine alerts using Streamlit
g. Orchestrate tasks using Apache Airflow

## Technologies Used:
--------------------
- Python
- Apache Airflow
- Pandas, NumPy
- Streamlit
- Matplotlib, Seaborn
- SQLite
- Docker
- Git & GitHub

## Installation instructions:
----------------------------
1. Clone the repo
```bash
git clone https://github.com/meenu1204/iot-predictive-maintenance-etl.git
cd iot-predictive-maintenance-etl

2.

## Running the pipeline 
-----------------------------
To run ETL script:
```bash
cd iot-predictive-maintenance-etl
python3 -m src.main.main
