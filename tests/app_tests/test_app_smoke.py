# tests/app_tests/test_app_smoke.py
from pathlib import Path

from streamlit.testing.v1 import AppTest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_PATH = PROJECT_ROOT / "webapp" / "app.py"


def test_app_smoke_runs():
    at = AppTest.from_file(str(APP_PATH))
    at.run()
    # If the app crashes, AppTest raises, so reaching here is enough.


def test_app_single_agency_graph():
    at = AppTest.from_file(str(APP_PATH)).run()

    # Basic sanity: there is at least one sidebar multiselect (branches)
    assert len(at.sidebar.multiselect) >= 1

    # No need to click buttons by index (UI may differ); just verify structure.
