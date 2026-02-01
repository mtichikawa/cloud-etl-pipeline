"""
tests/test_extract.py — Unit tests for extraction layer.
Uses unittest.mock to avoid real API calls.
"""

import json
import pytest
from unittest.mock import patch, MagicMock

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extract import extract_city_weather, extract_all_cities, save_raw_locally


MOCK_API_RESPONSE = {
    "daily": {
        "time": ["2026-02-01"],
        "temperature_2m_max": [12.5],
        "temperature_2m_min": [4.2],
        "temperature_2m_mean": [8.1],
        "precipitation_sum": [3.2],
        "wind_speed_10m_max": [28.0],
        "wind_gusts_10m_max": [45.0],
        "shortwave_radiation_sum": [5.1],
        "et0_fao_evapotranspiration": [0.9],
    }
}


@pytest.fixture
def mock_api():
    with patch("src.extract._get_with_retry", return_value=MOCK_API_RESPONSE) as m:
        yield m


def test_extract_city_returns_dict(mock_api):
    result = extract_city_weather("Portland", 45.5, -122.7, "America/Los_Angeles", "2026-02-01")
    assert isinstance(result, dict)


def test_extract_city_has_required_fields(mock_api):
    result = extract_city_weather("Portland", 45.5, -122.7, "America/Los_Angeles", "2026-02-01")
    assert result["city"] == "Portland"
    assert result["date"] == "2026-02-01"
    assert "temperature_2m_mean" in result
    assert "precipitation_sum" in result


def test_extract_city_flattens_daily_arrays(mock_api):
    result = extract_city_weather("Portland", 45.5, -122.7, "America/Los_Angeles", "2026-02-01")
    # Daily values should be scalars, not lists
    assert isinstance(result["temperature_2m_mean"], float)
    assert result["temperature_2m_mean"] == 8.1


def test_extract_city_returns_none_on_http_error():
    import requests
    with patch("src.extract._get_with_retry", side_effect=requests.HTTPError("404")):
        result = extract_city_weather("Portland", 45.5, -122.7, "America/Los_Angeles", "2026-02-01")
    assert result is None


def test_extract_city_returns_none_on_generic_error():
    with patch("src.extract._get_with_retry", side_effect=Exception("timeout")):
        result = extract_city_weather("Portland", 45.5, -122.7, "America/Los_Angeles", "2026-02-01")
    assert result is None


def test_extract_all_cities_returns_list(mock_api):
    results = extract_all_cities("2026-02-01")
    assert isinstance(results, list)
    assert len(results) == 10  # all cities


def test_extract_all_cities_skips_failed():
    call_count = 0
    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count <= 3:
            raise Exception("API error")
        return MOCK_API_RESPONSE

    with patch("src.extract._get_with_retry", side_effect=side_effect):
        results = extract_all_cities("2026-02-01")

    # First 3 cities fail, rest succeed
    assert len(results) == 7


def test_extract_includes_metadata(mock_api):
    result = extract_city_weather("Portland", 45.5, -122.7, "America/Los_Angeles", "2026-02-01")
    assert "fetched_at" in result
    assert "fetch_ms" in result
    assert "latitude" in result
    assert "longitude" in result


def test_save_raw_locally_creates_files(mock_api, tmp_path):
    import config
    original_raw = config.RAW_DIR
    config.RAW_DIR = tmp_path / "raw"
    config.RAW_DIR.mkdir()

    records = [{"city": "Portland", "date": "2026-02-01",
                "latitude": 45.5, "longitude": -122.7, "timezone": "America/Los_Angeles",
                "temperature_2m_mean": 8.1}]

    paths = save_raw_locally(records, "2026-02-01")
    assert len(paths) == 1
    assert paths[0].exists()

    config.RAW_DIR = original_raw
