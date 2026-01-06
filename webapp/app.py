"""Streamlit APP displaying sensors traffic temporal series
[Application live](https://bank-branch-footfall.streamlit.app/)"""

import calendar
import glob
from datetime import date, datetime, timedelta

import duckdb
import pandas as pd
import plotly.colors
import plotly.graph_objects as go
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


def get_multi_agency_data(agencies, counter_id, parquet_file, time_period):
    dfs = []
    for agency in agencies:
        df = get_sensor_dataframe(agency, counter_id, parquet_file, time_period)
        df["agency_name"] = agency
        dfs.append(df)
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()


def get_agency_footfall_all_sensors(agencies, parquet_file, time_period):
    dfs = []
    for agency in agencies:
        # Query all sensors for this agency and all columns
        query = f"""
            SELECT *
            FROM {parquet_file}
            WHERE agency_name = '{agency}'
              AND date >= '{time_period[0]}'::DATE AND date <= '{time_period[1]}'::DATE
        """
        df = duckdb.sql(query).df()
        if not df.empty:
            # Group by date and aggregate
            agg_df = (
                df.groupby("date")
                .agg(
                    {
                        "daily_visitor_count": "sum",
                        "avg_visits_4_weekday": "sum",
                        "prev_avg_4_visits": "sum",
                        "pct_change": "sum",  # wrong but temporary
                    }
                )
                .reset_index()
            )
            # need to recalculate the total percentage change with all sensors as it's a ratio (not sumable)
            agg_df["pct_change"] = 100 * (
                agg_df["daily_visitor_count"] / agg_df["prev_avg_4_visits"] - 1
            )
            agg_df["agency_name"] = agency
            dfs.append(agg_df)
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()


# -------------------------------------------------------------------------------------------------
# time selection


# Function to get start and end dates for a specific year
def get_year_dates(year=datetime.today().year):
    """Retourne la date de début et de fin pour une année donnée."""
    start_date = datetime(year, 1, 1)
    start_date = start_date.date()
    end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    end_date = end_date.date()

    st.success("Selected date range:")
    st.success(f"{start_date} to {end_date}")

    return start_date, end_date


# Function to get start and end dates for a specific month
def get_month_dates(month_name, year=datetime.today().year):
    """get start_date and end_date from a month choice"""
    month_num = list(calendar.month_name).index(month_name)
    start_date = datetime(year, month_num, 1)
    start_date = start_date.date()

    # Find the last day of the month
    if month_num == 12:  # December
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month_num + 1, 1) - timedelta(days=1)
    end_date = end_date.date()

    st.success("Selected date range:")
    st.success(f"{start_date} to {end_date}")

    return start_date, end_date


# Function to get start and end dates for a specific week number
def get_week_dates(week_number, year=datetime.today().year):
    """Return the start (Monday) and end (Sunday) dates for an ISO week number."""
    start_date = date.fromisocalendar(year, week_number, 1)  # Monday
    end_date = date.fromisocalendar(year, week_number, 7)  # Sunday
    st.success("Selected date range:")
    st.success(f"{start_date} to {end_date}")
    return start_date, end_date


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

    # st.success(f"Selected date range:")
    # st.success(f"{start_date} to {end_date}")

    return week_w


def get_time_period(year: int) -> tuple[date, date]:
    """Display a box to choose start_date and end_date within the selected year."""
    st.subheader("Select Date Range")
    min_date = date(year, 1, 1)
    max_date = date(year, 12, 31)

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

    if start_date > end_date:
        st.error("Error: End date must be after start date.")
    else:
        st.success(f"Selected date range: {start_date} to {end_date}")

    return start_date, end_date


# --------------------------------------------------------------------------------------------------


def get_sensor_dataframe(
    agency_n: str,
    counter_i: int,
    parquet_file: str,
    time_delta: tuple[date, date] = None,
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


def display_sensor_graph_with_checkboxes(
    df: pd.DataFrame, agency_n: str, counter_i: int
):
    st.subheader("Variables à afficher")
    show_daily = st.checkbox("Visiteurs quotidiens", value=True)
    # show_avg_weekday = st.checkbox("Moy. 4 mêmes jours (avg_visits_4_weekday)", value=False)
    show_prev_avg = st.checkbox("Moy. 4 mêmes jours précédents", value=False)
    show_pct_change = st.checkbox("Variation (%)", value=False)

    fig = go.Figure()

    if show_daily and "daily_visitor_count" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["daily_visitor_count"],
                mode="lines+markers",
                name="Visiteurs quotidiens",
                line=dict(width=2, color="#1f77b4", dash="solid"),
                marker=dict(symbol="circle"),
            )
        )

    # if show_avg_weekday and "avg_visits_4_weekday" in df.columns:
    #     fig.add_trace(go.Scatter(
    #         x=df["date"], y=df["avg_visits_4_weekday"],
    #         mode="lines",
    #         # mode="lines+markers",
    #         name="Moy. 4 mêmes jours",
    #         line=dict(width=2, color="#ff7f0e", dash="dash"),
    #         # marker=dict(symbol="diamond", size=10)
    #     ))

    if show_prev_avg and "prev_avg_4_visits" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["prev_avg_4_visits"],
                mode="lines",
                # mode="lines+markers",
                name="Moy. 4 visites précédentes",
                line=dict(width=2, color="#2ca02c", dash="dot"),
                # marker=dict(symbol="square", size=10)
            )
        )

    if show_pct_change and "pct_change" in df.columns:
        fig.add_trace(
            go.Bar(
                x=df["date"],
                y=df["pct_change"],
                name="Variation (%)",
                yaxis="y2",
                marker_color="#8da0cb",
                opacity=0.4,
            )
        )
        fig.update_layout(
            yaxis2=dict(
                title="Variation (%)", overlaying="y", side="right", showgrid=False
            )
        )

    fig.update_layout(
        title=f"Trafic journalier - {agency_n} (capteur {counter_i})",
        xaxis_title="Date",
        yaxis_title="Visiteurs quotidiens",
        legend_title="Afficher/Masquer",
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=40, r=40, t=60, b=40),
        height=600,
        width=1100,
    )
    st.plotly_chart(fig, use_container_width=True)


def display_comparison_graph_with_checkboxes(df: pd.DataFrame, agencies: list):
    st.subheader("Variables à afficher (pour toutes les agences)")
    show_daily = st.checkbox("Visiteurs quotidiens", value=True)
    # show_avg_weekday = st.checkbox("Moy. 4 mêmes jours (avg_visits_4_weekday)", value=False)
    show_prev_avg = st.checkbox("Moy. 4 mêmes jours précédents", value=False)
    show_pct_change = st.checkbox("Variation (%)", value=False)

    # Assign a unique color to each agency
    palette = plotly.colors.qualitative.Set1
    agency_colors = {
        agency: palette[i % len(palette)] for i, agency in enumerate(agencies)
    }

    fig = go.Figure()

    if df.empty:
        st.warning("No data available for the selected agencies and time period.")
        return

    for agency in agencies:
        df_ag = df[df["agency_name"] == agency]
        color = agency_colors[agency]
        if show_daily and "daily_visitor_count" in df_ag.columns:
            fig.add_trace(
                go.Scatter(
                    x=df_ag["date"],
                    y=df_ag["daily_visitor_count"],
                    mode="lines+markers",
                    name=f"{agency} - Visiteurs quotidiens",
                    line=dict(width=2, dash="solid", color=color),
                    marker=dict(symbol="circle", size=8, color=color),
                )
            )
        # if show_avg_weekday and "avg_visits_4_weekday" in df_ag.columns:
        #     fig.add_trace(go.Scatter(
        #         x=df_ag["date"], y=df_ag["avg_visits_4_weekday"],
        #         mode="lines",  # Only lines, no markers
        #         name=f"{agency} - Moy. 4 mêmes jours",
        #         line=dict(width=2, dash="dash", color=color)
        #     ))
        if show_prev_avg and "prev_avg_4_visits" in df_ag.columns:
            fig.add_trace(
                go.Scatter(
                    x=df_ag["date"],
                    y=df_ag["prev_avg_4_visits"],
                    mode="lines",  # Only lines, no markers
                    name=f"{agency} - Moy. 4 visites précédentes",
                    line=dict(width=2, dash="dot", color=color),
                )
            )
        if show_pct_change and "pct_change" in df_ag.columns:
            fig.add_trace(
                go.Bar(
                    x=df_ag["date"],
                    y=df_ag["pct_change"],
                    name=f"{agency} - Variation (%)",
                    yaxis="y2",
                    marker_color=color,
                    opacity=0.4,
                )
            )
            fig.update_layout(
                yaxis2=dict(
                    title="Variation (%)", overlaying="y", side="right", showgrid=False
                )
            )

    fig.update_layout(
        title="Comparaison du trafic journalier entre agences",
        xaxis_title="Date",
        yaxis_title="Valeur",
        legend_title="Agence / Variable",
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=40, r=40, t=60, b=40),
        height=600,
        width=1100,
    )
    st.plotly_chart(fig, use_container_width=True)


def display_average_bar_chart(df: pd.DataFrame):
    import plotly.express as px

    if df.empty:
        st.warning("No data to display.")
        return
    avg_df = df.groupby("agency_name")["daily_visitor_count"].mean().reset_index()
    fig = px.bar(
        avg_df,
        x="agency_name",
        y="daily_visitor_count",
        color="agency_name",
        title="Average Daily Footfall per Agency",
        labels={
            "daily_visitor_count": "Average Daily Visitors",
            "agency_name": "Agency",
        },
        height=400,
        width=900,
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

# Streamlit app font configuration
st.markdown(
    """
    <style>
    /* Change color of selected chips in multiselect */
    .stMultiSelect [data-baseweb="tag"] {
        background-color: #f5a623 !important;
        color: #fff !important;
    }
    /* Change the color of all checked checkboxes (the tick) */
    input[type="checkbox"]:checked {
        accent-color: #f5a623 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
# Streamlit app configuration
with st.sidebar:
    st.title("🏦 Bank Branch")

    with st.expander("Select branch", expanded=True):
        # Agency selection
        agency_names = sorted({row[0] for row in agency_sensor_list})
        # selected_agencies = st.multiselect("Bank branches", agency_names, default=agency_names[:1])
        selected_agencies = st.multiselect(
            "", agency_names, default=agency_names[:1], label_visibility="collapsed"
        )

        selected_sensor = None
        if len(selected_agencies) == 1:
            sensors = [
                str(row[1])
                for row in agency_sensor_list
                if row[0] == selected_agencies[0]
            ]
            # selected_sensor = st.selectbox("Select a sensor (optional)", ["All sensors"] + sensors)
            selected_sensor = st.selectbox(
                "", ["All sensors"] + sensors, label_visibility="collapsed"
            )

    st.title("📅 Time range")
    with st.expander("Select year(s)", expanded=True):
        # Year selection
        years = list(range(2000, datetime.today().year + 1))
        # selected_years = st.multiselect("Select years:", years, default=[datetime.today().year])
        default_year = 2024  # Default year to display
        selected_years = st.multiselect(
            "", years, default=[default_year], label_visibility="collapsed"
        )

        # --- Month selection box ---
        with st.expander("Select months", expanded=False):
            months = list(calendar.month_name)[1:]
            if "selected_months" not in st.session_state:
                st.session_state.selected_months = []
            if "selected_weeks" not in st.session_state:
                st.session_state.selected_weeks = []
            if not st.session_state.selected_weeks:
                selected_months = st.multiselect(
                    "Months:", months, default=st.session_state.selected_months
                )
                if selected_months:
                    st.session_state.selected_months = selected_months
                    st.session_state.selected_weeks = []
            else:
                selected_months = []

        # --- Week selection box ---
        with st.expander("Or select week numbers", expanded=False):
            week_numbers = list(range(1, 54))
            if not st.session_state.selected_months:
                selected_weeks = st.multiselect(
                    "Week numbers:",
                    week_numbers,
                    default=st.session_state.selected_weeks,
                )
                if selected_weeks:
                    st.session_state.selected_weeks = selected_weeks
                    st.session_state.selected_months = []
            else:
                selected_weeks = []

        # st.markdown("---")

        # --- Date range selection box ---
        with st.expander("Or use a custom date range", expanded=False):
            min_year = min(selected_years) if selected_years else 2000
            max_year = max(selected_years) if selected_years else datetime.today().year
            min_date = date(min_year, 1, 1)
            max_date = date(max_year, 12, 31)
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

# Data query logic
if not selected_agencies:
    st.info("Please select at least one agency to compare.")
elif not selected_years:
    st.info("Please select at least one year.")
else:
    if len(selected_agencies) == 1:
        agency = selected_agencies[0]
        if selected_sensor and selected_sensor != "All sensors":
            df = get_sensor_dataframe(
                agency, int(selected_sensor), PARQUET_FILE, (start_date, end_date)
            )
            df["agency_name"] = agency
            mode = "single_sensor"
        else:
            df = get_agency_footfall_all_sensors(
                [agency], PARQUET_FILE, (start_date, end_date)
            )
            mode = "single_agency"
    else:
        df = get_agency_footfall_all_sensors(
            selected_agencies, PARQUET_FILE, (start_date, end_date)
        )
        mode = "multi_agency"

    # Filtering
    if not df.empty and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df[
            (df["date"] >= pd.to_datetime(start_date))
            & (df["date"] <= pd.to_datetime(end_date))
            & (df["date"].dt.year.isin(selected_years))
        ]
        if selected_months:
            selected_month_nums = [
                list(calendar.month_name).index(m) for m in selected_months
            ]
            df = df[df["date"].dt.month.isin(selected_month_nums)]
        if selected_weeks:
            df = df[df["date"].dt.isocalendar().week.isin(selected_weeks)]

    # Buttons to display graph XOR data
    if "show_data" not in st.session_state:
        st.session_state.show_data = False

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("Graph"):
            st.session_state.show_data = False
    with col2:
        if st.button("Data"):
            st.session_state.show_data = True

    if df.empty:
        st.warning("No data available for the selected agencies and time period.")
    else:
        if not st.session_state.show_data:
            if mode == "single_sensor":
                display_sensor_graph_with_checkboxes(df, agency, int(selected_sensor))
            else:
                display_comparison_graph_with_checkboxes(df, selected_agencies)
        else:
            display_sensor_dataframe(df)
