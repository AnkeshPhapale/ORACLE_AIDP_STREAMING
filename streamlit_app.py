import os
import time

import streamlit as st
from pyspark.sql import SparkSession

st.set_page_config(page_title="Smart Meter Streaming", page_icon="⚡", layout="wide")

@st.cache_resource
def get_spark():
    url = os.getenv("SPARK_CONNECT_URL")
    return SparkSession.builder.remote(url).getOrCreate() if url else SparkSession.builder.getOrCreate()

def scalar(spark, sql):
    return spark.sql(sql).collect()[0][0]

def pdf(spark, sql):
    return spark.sql(sql).toPandas()

catalog = os.getenv("AIDP_CATALOG", "aidp_poc")
seconds = st.sidebar.selectbox("Refresh every", (10, 30, 60, 120), index=1)
auto = st.sidebar.toggle("Auto refresh", value=True)
st.title("⚡ Smart Meter Streaming Operations")
st.caption("Catalog: " + catalog)

try:
    spark = get_spark()
    bronze = scalar(spark, f"SELECT COUNT(*) FROM {catalog}.bronze.meter_reading")
    silver = scalar(spark, f"SELECT COUNT(*) FROM {catalog}.silver.interval_reading")
    predictions = scalar(spark, f"SELECT COUNT(*) FROM {catalog}.ml.meter_predictions")
    latest = pdf(spark, f"""
      SELECT MAX(interval_start_utc) latest_interval, COUNT(DISTINCT meter_id) active_meters,
             COUNT(*) interval_rows, ROUND(SUM(consumption_kwh),2) interval_kwh,
             ROUND(MAX(demand_kw),2) max_demand_kw,
             SUM(CASE WHEN tamper_flag THEN 1 ELSE 0 END) tamper_alerts,
             SUM(CASE WHEN outage_minutes > 0 THEN 1 ELSE 0 END) outage_readings
      FROM {catalog}.silver.interval_reading
      WHERE interval_start_utc=(SELECT MAX(interval_start_utc) FROM {catalog}.silver.interval_reading)
    """)
    row = latest.iloc[0]
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Bronze events", f"{bronze:,}"); c2.metric("Silver readings", f"{silver:,}")
    c3.metric("Latest active meters", f"{int(row.active_meters):,}"); c4.metric("Predictions", f"{predictions:,}")
    st.subheader("Latest 15-minute interval")
    a,b,c,d = st.columns(4)
    a.metric("Interval", str(row.latest_interval)); b.metric("Consumption", f"{row.interval_kwh:,.2f} kWh")
    c.metric("Maximum demand", f"{row.max_demand_kw:,.2f} kW"); d.metric("Tamper / outage", f"{int(row.tamper_alerts)} / {int(row.outage_readings)}")
    trend = pdf(spark, f"""
      SELECT interval_start_utc, ROUND(SUM(consumption_kwh),2) total_kwh, ROUND(MAX(demand_kw),2) max_demand_kw
      FROM {catalog}.silver.interval_reading
      WHERE interval_start_utc >= current_timestamp() - INTERVAL 24 HOURS
      GROUP BY interval_start_utc ORDER BY interval_start_utc
    """)
    st.subheader("24-hour consumption trend")
    if trend.empty: st.info("No Silver data available yet.")
    else: st.line_chart(trend.set_index("interval_start_utc")[["total_kwh","max_demand_kw"]])
    st.subheader("Latest interval by customer segment")
    st.dataframe(pdf(spark, f"""
      SELECT customer_segment, COUNT(*) meters, ROUND(SUM(consumption_kwh),2) kwh, ROUND(MAX(demand_kw),2) max_demand_kw
      FROM {catalog}.silver.interval_reading
      WHERE interval_start_utc=(SELECT MAX(interval_start_utc) FROM {catalog}.silver.interval_reading)
      GROUP BY customer_segment ORDER BY customer_segment
    """), use_container_width=True, hide_index=True)
    st.subheader("Recent ML predictions")
    st.dataframe(pdf(spark, f"""
      SELECT model_version, meter_id, interval_start_utc, ROUND(prediction_kwh,4) prediction_kwh, scored_at
      FROM {catalog}.ml.meter_predictions ORDER BY scored_at DESC LIMIT 25
    """), use_container_width=True, hide_index=True)
except Exception as error:
    st.error("The dashboard could not query Spark.")
    st.code(str(error))
    st.info("Run Streamlit in the AIDP/Spark environment or configure SPARK_CONNECT_URL.")

if auto:
    time.sleep(seconds)
    st.rerun()
