import duckdb

import streamlit as st


# Read the parquet file to df
parquet_file= "'data/filtered/2024_agencies_daily_visitor_count/part-00000-0e27bd92-4b26-465a-9cf9-180b61966469-c000.snappy.parquet'"

# parquet_file = "'app/1.parquet'"

query = f"""
SELECT * FROM {parquet_file} LIMIT 10
"""
duckdb.sql(query).show()
# sensor_list = duckdb.sql(query)

# Display a list of all sensors
# with st.sidebar:

#

