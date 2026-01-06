import pandas as pd
from datetime import date
from unittest.mock import MagicMock, patch

from webapp import app



def _sample_df():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(
                [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]
            ),
            "daily_visitor_count": [10, 20, 15],
            "prev_avg_4_visits": [8, 18, 14],
            "pct_change": [25.0, 11.1, -7.1],
            "agency_name": ["A", "A", "A"],
        }
    )


@patch("webapp.app.st")
def test_display_average_bar_chart_non_empty(mock_st):
    df = _sample_df()
    app.display_average_bar_chart(df)
    # Should not show a warning, but should call plotly_chart
    assert not mock_st.warning.called
    mock_st.plotly_chart.assert_called_once()


@patch("webapp.app.st")
def test_display_average_bar_chart_empty(mock_st):
    df = _sample_df().iloc[0:0]
    app.display_average_bar_chart(df)
    mock_st.warning.assert_called_once()
    mock_st.plotly_chart.assert_not_called()


@patch("webapp.app.st")
def test_display_sensor_dataframe(mock_st):
    df = _sample_df()
    app.display_sensor_dataframe(df)
    mock_st.dataframe.assert_called_once_with(df, use_container_width=True)


@patch("webapp.app.st")
def test_display_sensor_graph_with_checkboxes(mock_st):
    df = _sample_df()

    # Configure checkboxes to return True, False, True (daily + pct_change)
    mock_st.checkbox.side_effect = [True, False, True]
    mock_st.subheader = MagicMock()

    app.display_sensor_graph_with_checkboxes(df, "A", 1)

    # Should create one line trace and one bar trace and call plotly_chart once
    mock_st.plotly_chart.assert_called_once()


@patch("webapp.app.st")
def test_display_comparison_graph_with_checkboxes_empty(mock_st):
    df = _sample_df().iloc[0:0]
    app.display_comparison_graph_with_checkboxes(df, ["A", "B"])
    mock_st.warning.assert_called_once()
    mock_st.plotly_chart.assert_not_called()


@patch("webapp.app.st")
def test_display_comparison_graph_with_checkboxes_non_empty(mock_st):
    df = _sample_df()
    df2 = _sample_df()
    df2["agency_name"] = "B"
    df = pd.concat([df, df2], ignore_index=True)

    mock_st.checkbox.side_effect = [True, False, False]
    app.display_comparison_graph_with_checkboxes(df, ["A", "B"])

    mock_st.plotly_chart.assert_called_once()
