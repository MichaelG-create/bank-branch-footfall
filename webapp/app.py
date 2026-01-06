"""Streamlit app displaying bank branch footfall time series.

Public app:
https://bank-branch-footfall.streamlit.app/
"""

import calendar
import glob
from datetime import date, datetime, timedelta

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# --------------------------------------------------------------------------------------
# Page & Plotly config
# --------------------------------------------------------------------------------------

st.set_page_config(
    page_title="Bank Branch Footfall",
    page_icon="🏦",
    layout="wide",
)

# Global Plotly template to use for all figures
PLOTLY_TEMPLATE = "plotly_white"
# Available templates:
# "plotly" - default plotly theme
# "plotly_white" - white background, clean
# "plotly_dark" - dark theme
# "ggplot2" - ggplot2 style from R
# "seaborn" - seaborn style
# "simple_white" - minimal white theme
# "presentation" - for presentations
# "xgridoff" - no vertical grid
# "ygridoff" - no horizontal grid
# "gridon" - show all grids
# "none" - no template

# Professional Banking Theme Colors
PRIMARY_COLOR = "#1C3F5E"      # Deep navy blue - trust, professionalism
ACCENT_COLOR = "#D4AF37"       # Gold - premium, banking heritage
SUCCESS_COLOR = "#2E7D32"      # Forest green - growth, positive metrics
WARNING_COLOR = "#F57C00"      # Amber - attention needed
DANGER_COLOR = "#C62828"       # Deep red - alerts, negative metrics
TEXT_COLOR = "#2C3E50"         # Dark gray-blue for text
BACKGROUND = "#F5F7FA"         # Soft gray-blue background
CARD_BG = "#FFFFFF"            # White for cards/panels

# --------------------------------------------------------------------------------------
# Data helpers
# --------------------------------------------------------------------------------------

def get_sensor_list(parquet_file: str) -> list[tuple[str, int]]:
    """Read the parquet table and return list of (agency_name, counter_id)."""
    query = f"""
        WITH agency_sensors_cte AS (
          SELECT DISTINCT agency_name, counter_id
          FROM {parquet_file}
          ORDER BY agency_name, counter_id
        )
        SELECT agency_name, counter_id
        FROM agency_sensors_cte;
    """
    return duckdb.sql(query).fetchall()


def get_agency_footfall_all_sensors(
    agencies: list[str], parquet_file: str, time_period: tuple[date, date]
) -> pd.DataFrame:
    """Aggregate daily footfall across all sensors for each agency."""
    dfs: list[pd.DataFrame] = []

    for agency in agencies:
        query = f"""
            SELECT *
            FROM {parquet_file}
            WHERE agency_name = '{agency}'
              AND date >= '{time_period[0]}'::DATE
              AND date <= '{time_period[1]}'::DATE
        """
        df = duckdb.sql(query).df()
        if df.empty:
            continue

        agg_df = (
            df.groupby("date")
            .agg(
                {
                    "daily_visitor_count": "sum",
                    "avg_visits_4_weekday": "sum",
                    "prev_avg_4_visits": "sum",
                    "pct_change": "sum",
                }
            )
            .reset_index()
        )
        agg_df["pct_change"] = 100 * (
            agg_df["daily_visitor_count"] / agg_df["prev_avg_4_visits"] - 1
        )
        agg_df["agency_name"] = agency
        dfs.append(agg_df)

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def get_sensor_dataframe(
    agency_n: str,
    counter_i: int,
    parquet_file: str,
    time_delta: tuple[date, date] | None = None,
) -> pd.DataFrame:
    """Return dataframe for a given agency and sensor, optionally filtered by date range."""
    if time_delta is None:
        query = f"""
            SELECT *
            FROM {parquet_file}
            WHERE agency_name = '{agency_n}'
              AND counter_id = {counter_i}
            ORDER BY agency_name, counter_id, date;
        """
    else:
        query = f"""
            SELECT *
            FROM {parquet_file}
            WHERE agency_name = '{agency_n}'
              AND counter_id = {counter_i}
              AND date >= '{time_delta[0]}'::DATE
              AND date <= '{time_delta[1]}'::DATE
            ORDER BY agency_name, counter_id, date;
        """

    return duckdb.sql(query).df()


# --------------------------------------------------------------------------------------
# Display helpers
# --------------------------------------------------------------------------------------

def display_sensor_dataframe(df: pd.DataFrame) -> None:
    """Display the dataframe of the selected sensor / agencies."""
    st.dataframe(df, use_container_width=True)


def display_sensor_graph_with_checkboxes(
    df: pd.DataFrame, agency_n: str, counter_i: int
) -> None:
    """Display a graph for a single sensor with toggleable series."""
    st.subheader("Variables à afficher")
    show_daily = st.checkbox("Visiteurs quotidiens", value=True)
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
                line=dict(width=3, shape="spline", color=PRIMARY_COLOR),
                marker=dict(size=6, color=PRIMARY_COLOR),
            )
        )

    if show_prev_avg and "prev_avg_4_visits" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["prev_avg_4_visits"],
                mode="lines",
                name="Moy. 4 visites précédentes",
                line=dict(width=2, dash="dash", color=ACCENT_COLOR),
            )
        )

    if show_pct_change and "pct_change" in df.columns:
        colors = [SUCCESS_COLOR if x >= 0 else DANGER_COLOR for x in df["pct_change"]]
        fig.add_trace(
            go.Bar(
                x=df["date"],
                y=df["pct_change"],
                name="Variation (%)",
                yaxis="y2",
                opacity=0.5,
                marker=dict(color=colors),
            )
        )
        fig.update_layout(
            yaxis2=dict(
                title="Variation (%)",
                overlaying="y",
                side="right",
                showgrid=False,
            )
        )

    fig.update_layout(
        title=f"Trafic journalier - {agency_n} (capteur {counter_i})",
        xaxis_title="Date",
        yaxis_title="Visiteurs quotidiens",
        legend_title="Afficher/Masquer",
        template=PLOTLY_TEMPLATE,
        hovermode="x unified",
        margin=dict(l=40, r=40, t=60, b=40),
        height=600,
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color=TEXT_COLOR),
    )
    st.plotly_chart(fig, use_container_width=True)


def display_comparison_graph_with_checkboxes(
    df: pd.DataFrame, agencies: list[str]
) -> None:
    st.subheader("Variables à afficher (pour toutes les agences)")
    show_daily = st.checkbox("Visiteurs quotidiens", value=True)
    show_prev_avg = st.checkbox("Moy. 4 mêmes jours précédents", value=False)
    show_pct_change = st.checkbox("Variation (%)", value=False)

    if df.empty:
        st.warning("No data available for the selected agencies and time period.")
        return

    # Modern color palette
    color_palette = ["#2E86AB", "#A23B72", "#F18F01", "#C73E1D", "#6A994E", "#BC4B51"]
    agency_colors = {
        agency: color_palette[i % len(color_palette)] for i, agency in enumerate(agencies)
    }

    fig = go.Figure()

    for agency in agencies:
        df_ag = df[df["agency_name"] == agency]
        color = agency_colors[agency]

        if show_daily and "daily_visitor_count" in df_ag.columns:
            fig.add_trace(
                go.Scatter(
                    x=df_ag["date"],
                    y=df_ag["daily_visitor_count"],
                    mode="lines",
                    name=f"{agency} - Visiteurs quotidiens",
                    line=dict(
                        color=color, 
                        width=3, 
                        dash="solid",
                        shape="spline",
                    ),
                )
            )

        if show_prev_avg and "prev_avg_4_visits" in df_ag.columns:
            fig.add_trace(
                go.Scatter(
                    x=df_ag["date"],
                    y=df_ag["prev_avg_4_visits"],
                    mode="lines",
                    name=f"{agency} - Moy. 4 visites précédentes",
                    line=dict(color=color, width=2, dash="dash"),
                    opacity=0.7,
                )
            )

        if show_pct_change and "pct_change" in df_ag.columns:
            fig.add_trace(
                go.Bar(
                    x=df_ag["date"],
                    y=df_ag["pct_change"],
                    name=f"{agency} - Variation (%)",
                    yaxis="y2",
                    marker=dict(
                        color=df_ag["pct_change"],
                        colorscale="RdBu_r",
                        cmin=-100,
                        cmax=100,
                        colorbar=dict(
                            title="% change",
                            xanchor="left",
                        ),
                    ),
                    opacity=0.6,
                )
            )

    if show_pct_change:
        fig.update_layout(
            yaxis2=dict(
                title="Variation (%)",
                overlaying="y",
                side="right",
                showgrid=False,
            )
        )

    fig.update_layout(
        title="Comparaison du trafic journalier entre agences",
        xaxis_title="Date",
        yaxis_title="Valeur",
        legend_title="Agence / Variable",
        template=PLOTLY_TEMPLATE,
        hovermode="x unified",
        margin=dict(l=40, r=40, t=60, b=40),
        height=600,
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    st.plotly_chart(fig, use_container_width=True)


def display_average_bar_chart(df: pd.DataFrame) -> None:
    """Display average daily visitors per agency."""
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
        template=PLOTLY_TEMPLATE,
        color_discrete_sequence=[PRIMARY_COLOR, "#2E5C7F", "#4A7BA7", ACCENT_COLOR, "#8B7355"],
    )
    fig.update_layout(font=dict(color=TEXT_COLOR))
    st.plotly_chart(fig, use_container_width=True)


# --------------------------------------------------------------------------------------
# Init parquet + agency list
# --------------------------------------------------------------------------------------

PROJECT_PATH = ""
FOLDER_PATH = PROJECT_PATH + "data/filtered/parquet/"
parquet_files = glob.glob(FOLDER_PATH + "*.parquet")
if not parquet_files:
    st.error("No parquet files found in data/filtered/parquet/.")
    st.stop()

PARQUET_FILE = f"'{parquet_files[0]}'"
agency_sensor_list = get_sensor_list(PARQUET_FILE)


# --------------------------------------------------------------------------------------
# Global CSS styling
# --------------------------------------------------------------------------------------

st.markdown(
    """
    <style>
    /* Main background - soft professional gray-blue */
    .stApp {
        background-color: #F5F7FA;
    }
    
    /* Sidebar styling - white with subtle border */
    [data-testid="stSidebar"] {
        background-color: #FFFFFF;
        border-right: 2px solid #E1E8ED;
    }
    
    /* Multiselect chips - deep navy blue (banking professional) */
    .stMultiSelect [data-baseweb="tag"] {
        background-color: #1C3F5E !important;
        color: #FFFFFF !important;
        border-radius: 6px !important;
        font-weight: 500 !important;
    }
    
    /* Checkbox accent color - deep navy */
    input[type="checkbox"]:checked {
        accent-color: #1C3F5E !important;
    }
    
    /* Button styling - professional navy with gold hover */
    .stButton button {
        background-color: #1C3F5E;
        color: white;
        border: 2px solid #1C3F5E;
        border-radius: 8px;
        padding: 0.6rem 1.5rem;
        font-weight: 600;
        font-size: 1rem;
        transition: all 0.3s ease;
        letter-spacing: 0.5px;
    }
    
    .stButton button:hover {
        background-color: #D4AF37;
        border-color: #D4AF37;
        color: #1C3F5E;
        box-shadow: 0 4px 12px rgba(212, 175, 55, 0.3);
        transform: translateY(-2px);
    }
    
    /* Expander styling - professional gray */
    .streamlit-expanderHeader {
        background-color: #F8F9FA;
        border-radius: 8px;
        font-weight: 600;
        color: #2C3E50;
        border: 1px solid #E1E8ED;
    }
    
    .streamlit-expanderHeader:hover {
        background-color: #E8EDF2;
    }
    
    /* Dataframe styling */
    .stDataFrame {
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
    }
    
    /* Headers - professional dark blue-gray */
    h1, h2, h3 {
        color: #2C3E50;
        font-weight: 700;
    }
    
    /* Sidebar title - navy with gold accent */
    [data-testid="stSidebar"] h1 {
        color: #1C3F5E;
        border-bottom: 3px solid #D4AF37;
        padding-bottom: 0.5rem;
        margin-bottom: 1.5rem;
    }
    
    /* Metrics and info boxes */
    [data-testid="stMetricValue"] {
        color: #1C3F5E;
        font-weight: 700;
    }
    
    /* Warning/Info boxes with banking colors */
    .stAlert {
        border-radius: 8px;
        border-left: 4px solid #1C3F5E;
    }
    
    /* Selectbox and input styling */
    .stSelectbox > div > div,
    .stMultiSelect > div > div,
    .stDateInput > div > div {
        border-radius: 8px;
        border-color: #D1D5DB;
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        color: #2C3E50;
        font-weight: 600;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: #1C3F5E;
        color: white;
    }
    
    /* Subtle shadow for main content cards */
    .main .block-container {
        padding: 2rem;
        background-color: white;
        border-radius: 12px;
        box-shadow: 0 2px 12px rgba(0, 0, 0, 0.05);
        margin: 1rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# --------------------------------------------------------------------------------------
# Sidebar
# --------------------------------------------------------------------------------------

with st.sidebar:
    st.title("🏦 Bank Branch")

    with st.expander("Select branch", expanded=True):
        agency_names = sorted({row[0] for row in agency_sensor_list})
        selected_agencies = st.multiselect(
            "Bank branches",
            agency_names,
            default=agency_names[:1],
            label_visibility="collapsed",
        )

        selected_sensor: str | None = None
        if len(selected_agencies) == 1:
            sensors = [
                str(row[1])
                for row in agency_sensor_list
                if row[0] == selected_agencies[0]
            ]
            selected_sensor = st.selectbox(
                "Select a sensor (optional)",
                ["All sensors"] + sensors,
                label_visibility="collapsed",
            )

    st.title("📅 Time range")
    with st.expander("Select year(s)", expanded=True):
        years = list(range(2000, datetime.today().year + 1))
        default_year = 2024
        selected_years = st.multiselect(
            "Select years",
            years,
            default=[default_year],
            label_visibility="collapsed",
        )

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


# --------------------------------------------------------------------------------------
# Main logic
# --------------------------------------------------------------------------------------

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

    if not df.empty and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df[
            (df["date"] >= pd.to_datetime(start_date))
            & (df["date"] <= pd.to_datetime(end_date))
            & (df["date"].dt.year.isin(selected_years))
        ]

        if "selected_months" in st.session_state and st.session_state.selected_months:
            selected_month_nums = [
                list(calendar.month_name).index(m)
                for m in st.session_state.selected_months
            ]
            df = df[df["date"].dt.month.isin(selected_month_nums)]

        if "selected_weeks" in st.session_state and st.session_state.selected_weeks:
            df = df[df["date"].dt.isocalendar().week.isin(st.session_state.selected_weeks)]

    if "show_data" not in st.session_state:
        st.session_state.show_data = False

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("📊 Graph"):
            st.session_state.show_data = False
    with col2:
        if st.button("📋 Data"):
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