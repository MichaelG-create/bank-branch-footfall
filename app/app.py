""" Streamlit APP displaying sensors traffic temporal series """

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st


def get_sensor_list(parquet_file: str) -> list[tuple[str, int]]:
    """reads the parquet file and returns the list of (agencies, sensors)"""
    # pylint: disable=C0303
    query = f"""
            WITH agency_sensors_cte AS (  
              SELECT DISTINCT agency_name, counter_id 
              FROM {parquet_file} 
              -- LIMIT 1000
              ORDER BY agency_name, counter_id)
            
            SELECT agency_name, counter_id
            FROM agency_sensors_cte;
            """
    # duckdb.sql(query).show()
    agency_sensor = duckdb.sql(query).fetchall()
    return agency_sensor


def get_sensor_chosen(agency_sensor_lst: list[tuple[str, int]]) -> tuple[str, int]:
    """from the list agency_sensor_lst, display a selectbox
    returns the tuple (agency_name, counter_id) chosen by the user"""
    agency_sensor_list_str = [
        f"Agence {row[0]} - compteur # {row[1]}" for row in agency_sensor_lst
    ]

    sensor_chosen = st.selectbox("Choisir un capteur", agency_sensor_list_str)

    agency_sensor_dict = dict(zip(agency_sensor_list_str, agency_sensor_lst))

    return agency_sensor_dict[sensor_chosen]


def get_sensor_dataframe(
    agency_n: str, counter_i: int, parquet_file: str
) -> pd.DataFrame:
    """get sensor dataframe"""
    # pylint: disable=C0303
    query = f"""
              SELECT * 
              FROM {parquet_file} 
              WHERE agency_name = '{agency_n}' and counter_id = {counter_i}::INTEGER
              ORDER BY agency_name, counter_id, date;
            """
    return duckdb.sql(query).df()


def display_sensor_dataframe(agency_n: str, counter_i: int, parquet_file: str):
    """displays the dataframe of the chosen sensor"""
    sensor_df = get_sensor_dataframe(agency_n, counter_i, parquet_file)
    st.dataframe(sensor_df)


def display_history_graph_for_sensor(agency_n: str, counter_i: int, parquet_file: str):
    """displays the history graph of the chosen sensor"""
    df = get_sensor_dataframe(agency_n, counter_i, parquet_file)
    ## Create a bar chart using Plotly
    fig = px.line(df, x="date", y="daily_visitor_count")  # color='City',

    fig.update_layout(
        title=f"Daily traffic for agency {agency_n} - sensor {counter_i}",
        xaxis_title="Date",
        yaxis_title="Daily visitors",
    )
    ## Display the figure in Streamlit
    st.plotly_chart(fig)


if __name__ == "__main__":

    # Read the parquet file directly in duckdb (memory costless)
    PARQUET_FILE = (
        "'data/filtered/2024_agencies_daily_visitor_count/"
        "part-00000-0e27bd92-4b26-465a-9cf9-180b61966469-c000.snappy.parquet'"
    )

    agency_sensor_list = get_sensor_list(PARQUET_FILE)

    # Display a list of all sensors to be chosen
    # find the corresponding sensor agency_name and counter_id
    agency, sensor = get_sensor_chosen(agency_sensor_list)

    # print(agency, sensor, PARQUET_FILE)
    display_sensor_dataframe(agency, sensor, PARQUET_FILE)

    display_history_graph_for_sensor(agency, sensor, PARQUET_FILE)
