from pathlib import Path
import requests
from typing import Any, Dict

import pandas as pd
import pytest

from bank_footfall.etl import extract as extract_mod
from bank_footfall.etl.extract import FootfallExtractor, FootfallAPIResponse


def test_api_response_validation():
    data = {
        "agency_name": "Main Branch",
        "date_time": "2025-01-01 10:00",
        "visitor_count": 10,
        "unit": "visitors",
        "counter_id": 1,
    }

    resp = FootfallAPIResponse(**data)

    assert resp.agency_name == "Main Branch"
    assert resp.visitor_count == 10
    assert resp.counter_id == 1


def test_extract_footfall_data_mocks_api(monkeypatch):
    extractor = FootfallExtractor(api_base_url="http://fake-api")

    class DummyResponse:
        status_code = 200

        def __init__(self, json_data):
            self._json = json_data
            self.request = type("Req", (), {"url": "http://fake-url"})()

        def json(self):
            return self._json

        def raise_for_status(self):
            if self.status_code != 200:
                raise Exception("HTTP error")

    captured_url = {}
    captured_params = {}

    def fake_get(url, params=None, **kwargs):
        captured_url["url"] = url
        captured_params["params"] = params
        json_data = {
            "agency_name": params["agency_name"],
            "date_time": params["date_time"],
            "visitor_count": 42,
            "unit": params["count_unit"],
            "counter_id": params["counter_id"],
        }
        return DummyResponse(json_data)

    monkeypatch.setattr(extractor.session, "get", fake_get)

    result = extractor.extract_footfall_data(
        date_time="2025-01-01 10:00",
        agency_name="Main Branch",
        counter_id=1,
        count_unit="visitors",
    )

    assert captured_url["url"].endswith("/get_visitor_count")
    assert captured_params["params"]["agency_name"] == "Main Branch"

    assert len(result) == 1
    rec = result[0]
    assert rec["visitor_count"] == 42
    assert rec["unit"] == "visitors"
    assert rec["counter_id"] == 1


def test_save_to_csv_writes_file(tmp_path: Path):
    extractor = FootfallExtractor(api_base_url="http://fake-api")

    data = [
        {
            "agency_name": "Main Branch",
            "date_time": "2025-01-01 10:00",
            "visitor_count": 10,
            "unit": "visitors",
            "counter_id": 1,
        }
    ]

    output_path = tmp_path / "data" / "raw" / "footfall_data.csv"
    extractor.save_to_csv(data, output_path)

    assert output_path.is_file()

    df = pd.read_csv(output_path)
    assert len(df) == 1
    assert df.loc[0, "agency_name"] == "Main Branch"
    assert df.loc[0, "visitor_count"] == 10


def test_extract_footfall_data_raises_on_http_error(monkeypatch):
    extractor = FootfallExtractor(api_base_url="http://fake-api")

    class DummyResponse:
        status_code = 500
        request = type("Req", (), {"url": "http://fake-url"})()

        def json(self):
            return {}

        def raise_for_status(self):
            raise requests.HTTPError("boom")

    def fake_get(url, params=None, **kwargs):
        return DummyResponse()

    monkeypatch.setattr(extractor.session, "get", fake_get)

    with pytest.raises(requests.HTTPError):
        extractor.extract_footfall_data(
            date_time="2025-01-01 10:00",
            agency_name="Main Branch",
            counter_id=1,
            count_unit="visitors",
        )


def test_save_to_csv_raises_on_dataframe_failure(monkeypatch, tmp_path):
    extractor = FootfallExtractor(api_base_url="http://fake-api")

    data = [
        {
            "agency_name": "Main Branch",
            "date_time": "2025-01-01 10:00",
            "visitor_count": 10,
            "unit": "visitors",
            "counter_id": 1,
        }
    ]

    output_path = tmp_path / "data" / "raw" / "footfall_data.csv"

    def fake_dataframe(_):
        raise ValueError("bad data")

    monkeypatch.setattr(pd, "DataFrame", fake_dataframe)

    with pytest.raises(ValueError):
        extractor.save_to_csv(data, output_path)


def test_main_uses_settings_and_calls_extractor(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # Redirect project_root so we do not touch the real repo
    monkeypatch.setattr(extract_mod.settings, "project_root", tmp_path)

    # Replace FootfallExtractor() with a controlled fake instance
    real_extractor = FootfallExtractor(api_base_url="http://fake-api")
    monkeypatch.setattr(extract_mod, "FootfallExtractor", lambda: real_extractor)

    called: Dict[str, Any] = {}

    def fake_extract(date_time: str, agency_name: str, counter_id: int, count_unit: str):
        called["extract"] = {
            "date_time": date_time,
            "agency_name": agency_name,
            "counter_id": counter_id,
            "count_unit": count_unit,
        }
        return [
            {
                "agency_name": agency_name,
                "date_time": date_time,
                "visitor_count": 10,
                "unit": count_unit,
                "counter_id": counter_id,
            }
        ]

    def fake_save(data, output_path: Path) -> None:
        called["save"] = output_path

    monkeypatch.setattr(real_extractor, "extract_footfall_data", fake_extract)
    monkeypatch.setattr(real_extractor, "save_to_csv", fake_save)

    # Act
    extract_mod.main()

    # Assert: main wired arguments and path correctly
    assert "extract" in called
    assert called["extract"]["counter_id"] == 0
    assert called["extract"]["count_unit"] == "visitors"

    expected_path = tmp_path / "data" / "raw" / "footfall_data.csv"
    assert called["save"] == expected_path
