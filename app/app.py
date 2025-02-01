""" Streamlit APP displaying sensors traffic temporal series """

from datetime import date

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


def get_agency_chosen(agency_sensor_lst: list[tuple[str, int]]) -> str:
    """from the list agency_sensor_lst, display a selectbox
    returns the agency_name chosen by the user"""
    agency_sensor_list_str = sorted({row[0] for row in agency_sensor_lst})

    agency_choice = st.selectbox("Choisir une agence", agency_sensor_list_str)

    return agency_choice


def get_sensor_chosen(agency_sensor_lst: list[tuple[str, int]], agency_n: str) -> int:
    """from the list agency_sensor_lst, display a selectbox for available sensors is
    returns the counter_id chosen by the user"""
    sensor_list_str = [f"{row[1]}" for row in agency_sensor_lst if row[0] == agency_n]
    sensor_chosen = st.selectbox("Choisir un capteur", sensor_list_str)

    return sensor_chosen


def get_min_max_dates(agency_n: str, counter_i: int, parquet_file: str):
    """returns min and max dates for current sensor"""
    sensor_df = get_sensor_dataframe(agency_n, counter_i, parquet_file)
    return (min(sensor_df["date"]), max(sensor_df["date"]))


def get_time_period(agency_n: str, counter_i: int, parquet_file: str) -> (date, date):
    """from the agency and sensor name,
    display a box to choose start_date and end_date
    having a min and max dates taken from the df for this sensor
    :returns: the min and max dates"""
    st.title("Select Date Range")

    # Define min and max date range
    min_date, max_date = get_min_max_dates(agency_n, counter_i, parquet_file)

    # Date range selection using calendar picker with min and max values
    start_date = st.date_input(
        "Start Date",
        value=min_date,
        min_value=min_date,
        max_value=max_date,
        format="YYYY-MM-DD",
    )
    end_date = st.date_input(
        "End Date",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
        format="YYYY-MM-DD",
    )

    # Ensure start date is before end date
    if start_date > end_date:
        st.error("Error: End date must be after start date.")
    else:
        st.success(f"Selected date range: {start_date} to {end_date}")

    return start_date, end_date


def get_sensor_dataframe(
    agency_n: str, counter_i: int, parquet_file: str, time_delta: (date, date) = None
) -> pd.DataFrame:
    """get sensor dataframe"""
    # pylint: disable=C0303

    if time_delta is None:
        query = f"""
                  SELECT * 
                  FROM {parquet_file} 
                  WHERE agency_name = '{agency_n}' and counter_id = {counter_i}
                  ORDER BY agency_name, counter_id, date;
                """
    else:
        query = f"""
                          SELECT * 
                          FROM {parquet_file} 
                          WHERE agency_name = '{agency_n}' and counter_id = {counter_i}
                          and date >= '{time_delta[0]}'::DATE and date <= '{time_delta[1]}'::DATE
                          ORDER BY agency_name, counter_id, date;
                        """

    return duckdb.sql(query).df()


def display_sensor_dataframe(df: pd.DataFrame):
    """displays the dataframe of the chosen sensor"""
    st.dataframe(df)


def display_daily_graph_for_sensor(agency_n: str, counter_i: int, df: pd.DataFrame):
    """displays the history graph of the chosen sensor"""
    # Melting the dataframe
    df_part = df[["date", "daily_visitor_count", "prev_avg_4_visits"]]
    df_melted = df_part.melt(
        id_vars="date", var_name="daily_count", value_name="counting_type"
    )
    # Creating the line chart
    fig = px.line(df_melted, x="date", y="counting_type", color="daily_count")

    fig.update_layout(
        title=f"Daily traffic for agency {agency_n} - sensor {counter_i}",
        xaxis_title="Date",
        yaxis_title="Daily visitors",
    )
    ## Display the figure in Streamlit
    st.plotly_chart(fig)


if __name__ == "__main__":

    # parquet file location (directly read in duckdb (memory costless))
    PARQUET_FILE = (
        "'data/filtered/2024_agencies_daily_visitor_count/"
        "part-00000-0e27bd92-4b26-465a-9cf9-180b61966469-c000.snappy.parquet'"
    )

    agency_sensor_list = get_sensor_list(PARQUET_FILE)

    with st.sidebar:
        st.subheader("Sensor selection")

        # Display a list of all sensors to be chosen
        # find the corresponding sensor agency_name and counter_id
        agency = get_agency_chosen(agency_sensor_list)
        sensor = get_sensor_chosen(agency_sensor_list, agency)
        time_period = get_time_period(agency, sensor, PARQUET_FILE)
        # time_period = get_month_selection(agency, sensor, PARQUET_FILE, time_period)
        # time_period = get_week_selection(agency, sensor, PARQUET_FILE, time_period)

        # choose a weekly traffic or monthly

    data_f = get_sensor_dataframe(agency, sensor, PARQUET_FILE, time_period)

    display_sensor_dataframe(data_f)

    display_daily_graph_for_sensor(agency, sensor, data_f)
