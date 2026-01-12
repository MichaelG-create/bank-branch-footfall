# Tests overview

This document summarizes the automated tests for the **Bank Branch Footfall** project: what is covered, what is not, and how to run the suite.

---

## Test layout

All tests live under the top-level `tests/` folder:

- `tests/`
  - `test_extract.py`
  - `test_etl_transform.py`
  - `test_config.py`
  - `test_models.py`
  - `test_agency.py`
  - `test_counter.py`
  - `app_tests/`
    - `test_app_smoke.py`
    - `test_data_helpers.py`
    - `test_display_helpers.py`

Pytest is used everywhere, with `pytest-cov` for coverage.

---

## ETL and core library tests

### Covered

- **API extraction (`tests/test_extract.py`)**
  - Pydantic models validate raw API responses.
  - `FootfallExtractor` builds correct request parameters and parses JSON responses.
  - CSV export writes expected columns and values.

- **Transform & load (`tests/test_etl_transform.py`)**
  - End‑to‑end pipeline run against a temporary project root:
    - Creates expected folders under `data/`.
    - Writes filtered parquet files.
    - Resulting parquet contains key columns (`date`, `agency_name`, `counter_id`, `daily_visitor_count`, etc.).

- **Domain & config (`tests/test_config.py`, `test_models.py`, `test_agency.py`, `test_counter.py`)**
  - Configuration (`Settings`) builds correct default paths and options.
  - Domain models (agency, counter, etc.) behave as expected.
  - Any additional invariants / validation specific to the core package.

### Missing / planned

- **Failure modes and robustness**
  - HTTP error handling and retry / backoff behavior for the extractor.
  - Schema drift in raw CSV / parquet (unexpected columns or types).
  - Partial data or gaps in time series.

- **Performance and scale**
  - Larger synthetic datasets for ETL performance and memory behavior.
  - Tests around partitioning / file counts in output parquet.

---

## Web app tests (`webapp/app.py`)

### Covered

- **Data helpers (`tests/app_tests/test_data_helpers.py`)**
  - `get_sensor_list`:
    - Returns expected `(agency_name, counter_id)` pairs from a test DuckDB table.
  - `get_agency_footfall_all_sensors`:
    - Aggregates daily metrics per agency over a date range.
    - Recomputes `pct_change` from aggregated values.
    - Returns empty DataFrame when no data matches.
  - `get_sensor_dataframe`:
    - Handles both unbounded and bounded date ranges.
    - Filters correctly by `agency_name` and `counter_id`.

- **Display helpers (`tests/app_tests/test_display_helpers.py`)**
  - `display_average_bar_chart`:
    - Shows a warning and skips plotting on empty data.
    - Calls `st.plotly_chart` once on non‑empty data.
  - `display_sensor_dataframe`:
    - Calls `st.dataframe` with `use_container_width=True`.
  - `display_sensor_graph_with_checkboxes`:
    - Respects checkbox states for which series are plotted.
    - Produces a Plotly figure and calls `st.plotly_chart`.
  - `display_comparison_graph_with_checkboxes`:
    - Handles empty DataFrame (warning, no plot).
    - Handles multi‑agency DataFrame and plots with checkboxes.

- **App smoke tests (`tests/app_tests/test_app_smoke.py`)**
  - `webapp/app.py` runs end‑to‑end via `streamlit.testing.v1.AppTest` without crashing.
  - Basic sidebar structure exists (e.g. branch selection widgets).
  - Provides a safety net against catastrophic regressions in the app module.

### Missing / planned

- **Filter interaction via AppTest**
  - Verify behavior when:
    - Selecting one vs. multiple agencies.
    - Selecting specific sensors vs. “All sensors”.
    - Choosing year(s), month(s), week(s), and custom date ranges.
  - Assert that the correct mode (`single_sensor`, `single_agency`, `multi_agency`) is exercised and that the expected display helper is called.

- **No‑data UX**
  - Scenarios where filters produce no rows:
    - Confirm warnings are shown (and no stack traces).
    - Ensure graphs are not rendered in that case.

- **Plot content validation (optional)**
  - Inspect Plotly figures for:
    - Expected trace types and names.
    - Presence of secondary axis when `pct_change` is enabled.
    - Consistent titles and axis labels.

- **Layout / CSS (out of scope)**
  - Visual details (CSS, colors, exact layout) are intentionally not tested.

---

## How to run tests

All commands are intended to be run from the project root and assume `uv` manages the environment.

### Full test suite (all tests, no coverage)

```bash
uv run pytest
```
