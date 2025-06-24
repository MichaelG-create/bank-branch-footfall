"""Streamlit APP displaying sensors traffic temporal series
[Application live](https://bank-branch-footfall.streamlit.app/)"""

import calendar
import glob
from datetime import date, datetime, timedelta

import duckdb
import pandas as pd
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


# -------------------------------------------------------------------------------------------------
# time selection


def get_min_max_dates(agency_n: str, counter_i: int, parquet_file: str):
    """returns min and max dates for current sensor"""
    sensor_df = get_sensor_dataframe(agency_n, counter_i, parquet_file)
    return min(sensor_df["date"]), max(sensor_df["date"])


# Function to get start and end dates for a specific year
def get_year_dates(year=datetime.today().year):
    """Retourne la date de début et de fin pour une année donnée."""
    start_date = datetime(year, 1, 1)
    end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    return start_date.date(), end_date.date()


# Function to get start and end dates for a specific month
def get_month_dates(month_name, year=datetime.today().year):
    """get start_date and end_date from a month choice"""
    month_num = list(calendar.month_name).index(month_name)
    start_date = datetime(year, month_num, 1)
    # Find the last day of the month
    if month_num == 12:  # December
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month_num + 1, 1) - timedelta(days=1)
    return start_date.date(), end_date.date()


# Function to get start and end dates for a specific week number
def get_week_dates(week_number, year=datetime.today().year):
    """get start_date and end_date from a week choice"""
    # Get the first day of the year
    first_day_of_year = datetime(year, 1, 1)
    # Find the first Sunday of the year
    first_sunday = first_day_of_year + timedelta(days=6 - first_day_of_year.weekday())
    # Calculate the start date of the week
    start_date = first_sunday + timedelta(weeks=week_number - 1)
    # Calculate the end date (Saturday)
    end_date = start_date + timedelta(days=6)
    return start_date.date(), end_date.date()


def get_month_period() -> str:
    """Option 1: Select a certain full month"""
    st.subheader("Select Month")
    months = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    month_m = st.selectbox("Select a full month:", months)
    return month_m


def get_weeks_period() -> str:
    """Option 2: Select a certain full week (week number)"""
    st.subheader("Select week")
    weeks = list(range(1, 53))  # Week numbers from 1 to 52
    week_w = st.selectbox("Select a full week:", weeks)
    return week_w


def get_time_period(agency_n: str, counter_i: int, parquet_file: str) -> (date, date):
    """from the agency and sensor name,
    display a box to choose start_date and end_date
    having a min and max dates taken from the df for this sensor
    :returns: the min and max dates"""
    st.subheader("Select Date Range")

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


# --------------------------------------------------------------------------------------------------


def get_sensor_dataframe(
    agency_n: str, counter_i: int, parquet_file: str, time_delta: (date, date) = None
) -> pd.DataFrame:
    """get sensor dataframe"""
    # pylint: disable=C0303
    print(
        f"Getting data for agency {agency_n} and sensor {counter_i} from {parquet_file}"
    )
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
    """Displays a prettier history graph of the chosen sensor"""
    import plotly.graph_objects as go

    fig = go.Figure()

    # print(f"df.dtypes  = {df.dtypes}")
    # # print(df[['date', 'pct_chge']].head())

    # Série principale : visiteurs quotidiens
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["daily_visitor_count"],
            mode="lines+markers",
            name="Visiteurs quotidiens",
            line=dict(width=2, color="#66c2a5"),
        )
    )

    # Moyenne mobile
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["prev_avg_4_visits"],
            mode="lines+markers",
            name="Moy. 4 visites précédentes",
            line=dict(width=2, color="#fc8d62"),
        )
    )

    # Variation en % sur axe secondaire
    if "pct_chge" in df.columns:
        fig.add_trace(
            go.Bar(
                x=df["date"],
                y=df["pct_chge"],
                name="Variation (%)",
                marker_color="#8da0cb",
                opacity=0.4,
                yaxis="y2",
            )
        )

    fig.update_layout(
        title=f"Trafic journalier - {agency_n} (capteur {counter_i})",
        xaxis_title="Date",
        yaxis=dict(title="Visiteurs quotidiens", side="left"),
        yaxis2=dict(
            title="Variation (%)",
            overlaying="y",
            side="right",
            showgrid=False,
            rangemode="tozero",
        ),
        legend_title="Type de comptage",
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=40, r=40, t=60, b=40),
        height=600,
        width=1100,
    )
    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    PROJECT_PATH = ""
    # parquet file location (directly read in duckdb (memory costless))
    FOLDER_PATH = PROJECT_PATH + "data/filtered/parquet/"
    parquet_files = glob.glob(FOLDER_PATH + "*.parquet")

    # Get file names (just the names without full paths)
    parquet_file_names = [file.split("/")[-1] for file in parquet_files]

    PARQUET_FILE = f"'{parquet_files[0]}'"
    print(f"Using parquet file: {PARQUET_FILE}")

    agency_sensor_list = get_sensor_list(PARQUET_FILE)

    with st.sidebar:
        st.title("Sensor selection")

        # Display a list of all sensors to be chosen
        # find the corresponding sensor agency_name and counter_id
        agency = get_agency_chosen(agency_sensor_list)
        sensor = get_sensor_chosen(agency_sensor_list, agency)

        # choose to see traffic weekly, monthly or in a defined window
        st.title("Time period selection")
        time_period_choice = st.selectbox(
            "Choose a time selection method ",
            [
                "year",
                "month",
                "week",
                "time period",
            ],
        )

        if time_period_choice == "time period":
            time_period = get_time_period(agency, sensor, PARQUET_FILE)
            st.write(f"Selected date range: {time_period[0]} to {time_period[1]}")

        elif time_period_choice == "year":
            # Optionally let user pick a year, or use current year
            # current_year = datetime.today().year
            current_year = 2024  # For testing purposes, set to a fixed year
            year = st.number_input(
                "Select year:",
                min_value=2000,
                max_value=current_year,
                value=current_year,
            )
            time_period = get_year_dates(year)
            st.write(
                f"Selected year ({year}): "
                f"Start date = {time_period[0]}, End date = {time_period[1]}"
            )

        elif time_period_choice == "month":
            month = get_month_period()
            time_period = get_month_dates(month, 2024)
            st.write(
                f"Selected month ({month}): "
                f"Start date = {time_period[0]}, End date = {time_period[1]}"
            )

        elif time_period_choice == "week":
            week = get_weeks_period()
            time_period = get_week_dates(week, 2024)
            st.write(
                f"Selected full week ({week}): "
                f"Start date = {time_period[0]}, End date = {time_period[1]}"
            )

    data_f = get_sensor_dataframe(agency, sensor, PARQUET_FILE, time_period)

    # Add this before displaying graph/data
    if "show_data" not in st.session_state:
        st.session_state.show_data = False

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("Graph"):
            st.session_state.show_data = False
    with col2:
        if st.button("Data"):
            st.session_state.show_data = True

    if not st.session_state.show_data:
        display_daily_graph_for_sensor(agency, sensor, data_f)
    else:
        display_sensor_dataframe(data_f)
