# dashboard_app.py

import pandas as pd
import sqlite3
import streamlit as st

DB_PATH = "data/processed/sensor_data.db"
ALERT_THRESHOLDS = {
    "avg_temperature": 90,
    "vibration_alerts": 3,
    "voltage_anomalies": 2
}

# Cached data loading
@st.cache_data
def load_kpis(DB_PATH) -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql("SELECT * FROM machine_kpis", conn)

# Load data
df = load_kpis(DB_PATH)
print(df)

# App title and KPI Charts
st.title("IoT Predictive Maintenance Dashboard")
st.metric("Machines Monitored", len(df))

st_metrics = ["avg_temperature", "vibration_alerts", "voltage_anomalies"]
for metric in st_metrics:
    st.bar_chart(df.set_index("machine_id")[[metric]])

# Alert section
st.subheader("Machine Alerts")

def get_machine_alerts(row):
    alerts = []
    if row["avg_temperature"] > ALERT_THRESHOLDS["avg_temperature"]:
        alerts.append(f" High temperature ")
    if row["vibration_alerts"] > ALERT_THRESHOLDS["vibration_alerts"]:
        alerts.append(f" Excessive vibration ")
    if row["voltage_anomalies"] > ALERT_THRESHOLDS["voltage_anomalies"]:
        alerts.append(f" Voltage instability ")
    return alerts

for _, row in df.iterrows():
    alerts = get_machine_alerts(row)
    if alerts:
        st.error(f"Machine {row['machine_id']}" + ",".join(alerts))
    else:
        st.success(f"Machine {row['machine_id']} All KPIs normal")