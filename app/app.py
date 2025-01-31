""" Streamlit APP displaying sensors traffic temporal series """

import duckdb
import streamlit as st

# Read the parquet file to df
PARQUET_FILE = (
    "'data/filtered/2024_agencies_daily_visitor_count/"
    "part-00000-0e27bd92-4b26-465a-9cf9-180b61966469-c000.snappy.parquet'"
)

# PARQUET_FILE = "'app/1.parquet'"

# pylint: disable=C0303
QUERY = f"""
WITH agency_sensors_cte AS (  
  SELECT DISTINCT agency_name, counter_id 
  FROM {PARQUET_FILE} 
  -- LIMIT 1000
  ORDER BY agency_name, counter_id)

SELECT agency_name || ' - counter #' || counter_id AS agency_counter
FROM agency_sensors_cte;
"""

# duckdb.sql(QUERY).show()
sensor_list = duckdb.sql(QUERY).fetchall()
sensor_list = [row[0] for row in sensor_list]
print(sensor_list)
print(type(sensor_list))

# Display a list of all sensors
# with st.sidebar:
st.selectbox("Choisir un capteur", sensor_list)
