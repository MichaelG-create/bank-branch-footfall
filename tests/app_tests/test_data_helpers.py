from datetime import date

import duckdb
import pytest

from webapp import app


@pytest.fixture(scope="module")
def duck_con():
    """In-memory DuckDB connection with a 'footfall' table."""
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE footfall AS SELECT * FROM (VALUES
            ('A', 1, '2024-01-01', 10, 8, 5, 100.0),
            ('A', 2, '2024-01-02', 20, 18, 10, 100.0),
            ('B', 1, '2024-01-01', 5, 4, 2, 150.0)
        ) AS t(agency_name, counter_id, date, daily_visitor_count,
               avg_visits_4_weekday, prev_avg_4_visits, pct_change)
        """
    )
    try:
        yield con
    finally:
        con.close()


def _patch_duckdb(monkeypatch: pytest.MonkeyPatch, con: duckdb.DuckDBPyConnection):
    def _sql(query: str):
        return con.execute(query)

    monkeypatch.setattr(duckdb, "sql", _sql)


def test_get_sensor_list(monkeypatch, duck_con):
    _patch_duckdb(monkeypatch, duck_con)
    sensors = app.get_sensor_list("footfall")
    assert ("A", 1) in sensors
    assert ("A", 2) in sensors
    assert ("B", 1) in sensors


def test_get_agency_footfall_all_sensors_single_agency(monkeypatch, duck_con):
    _patch_duckdb(monkeypatch, duck_con)
    start = date(2024, 1, 1)
    end = date(2024, 1, 2)

    df = app.get_agency_footfall_all_sensors(["A"], "footfall", (start, end))

    assert set(df["agency_name"].unique()) == {"A"}
    assert len(df) == 2

    for col in [
        "daily_visitor_count",
        "avg_visits_4_weekday",
        "prev_avg_4_visits",
        "pct_change",
    ]:
        assert col in df.columns

    row = df.sort_values("date").iloc[0]
    expected_pct = 100 * (row["daily_visitor_count"] / row["prev_avg_4_visits"] - 1)
    assert row["pct_change"] == expected_pct


def test_get_agency_footfall_all_sensors_empty(monkeypatch, duck_con):
    _patch_duckdb(monkeypatch, duck_con)
    start = date(1999, 1, 1)
    end = date(1999, 1, 2)

    df = app.get_agency_footfall_all_sensors(["Z"], "footfall", (start, end))
    assert df.empty


def test_get_sensor_dataframe_no_range(monkeypatch, duck_con):
    _patch_duckdb(monkeypatch, duck_con)
    df = app.get_sensor_dataframe("A", 1, "footfall", time_delta=None)
    assert not df.empty
    assert (df["agency_name"] == "A").all()
    assert (df["counter_id"] == 1).all()
    assert list(df.columns)


def test_get_sensor_dataframe_with_range(monkeypatch, duck_con):
    _patch_duckdb(monkeypatch, duck_con)
    start = date(2024, 1, 1)
    end = date(2024, 1, 1)

    df = app.get_sensor_dataframe("A", 1, "footfall", (start, end))
    assert len(df) == 1
    assert df.iloc[0]["date"].date() == start
