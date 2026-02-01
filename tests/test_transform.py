"""
tests/test_transform.py — Unit tests for the transformation layer.
No AWS dependencies.
"""

import pytest
import numpy as np
import pandas as pd

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.transform import (
    transform_records,
    heat_index_celsius,
    precipitation_category,
    wind_category,
    df_to_parquet_bytes,
)


def make_record(**kwargs):
    base = {
        "city": "Portland", "latitude": 45.5, "longitude": -122.7,
        "timezone": "America/Los_Angeles", "date": "2026-02-01",
        "temperature_2m_mean": 8.1, "temperature_2m_max": 12.5,
        "temperature_2m_min": 4.2, "precipitation_sum": 3.2,
        "wind_speed_10m_max": 28.0, "wind_gusts_10m_max": 45.0,
        "shortwave_radiation_sum": 5.1, "et0_fao_evapotranspiration": 0.9,
    }
    base.update(kwargs)
    return base


# ── heat_index ─────────────────────────────────────────────────────────────────

def test_heat_index_reasonable_value():
    hi = heat_index_celsius(25.0, humidity_pct=50)
    assert 20 < hi < 35   # should be near 25 for moderate conditions


def test_heat_index_returns_nan_for_none():
    hi = heat_index_celsius(None)
    assert np.isnan(hi)


def test_heat_index_cold_temp():
    hi = heat_index_celsius(5.0, humidity_pct=50)
    assert isinstance(hi, float)


# ── precipitation_category ────────────────────────────────────────────────────

def test_precipitation_none_category():
    assert precipitation_category(0)   == "none"
    assert precipitation_category(2)   == "light"
    assert precipitation_category(10)  == "moderate"
    assert precipitation_category(30)  == "heavy"
    assert precipitation_category(100) == "extreme"
    assert precipitation_category(None) == "unknown"
    assert precipitation_category(float("nan")) == "unknown"


def test_wind_category():
    assert wind_category(10)  == "calm"
    assert wind_category(30)  == "breezy"
    assert wind_category(60)  == "windy"
    assert wind_category(100) == "strong"
    assert wind_category(150) == "storm"
    assert wind_category(None) == "unknown"


# ── transform_records ─────────────────────────────────────────────────────────

def test_transform_returns_dataframe():
    records = [make_record()]
    df = transform_records(records)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


def test_transform_required_columns_present():
    records = [make_record()]
    df = transform_records(records)
    required = ["city", "date", "avg_temp_c", "max_temp_c", "min_temp_c",
                "precipitation_mm", "wind_speed_max_kmh", "heat_index",
                "is_extreme_temp", "is_heavy_rain", "is_high_wind",
                "run_id", "processed_at"]
    for col in required:
        assert col in df.columns, f"Missing column: {col}"


def test_transform_derived_metrics():
    records = [make_record(temperature_2m_max=12.5, temperature_2m_min=4.2)]
    df = transform_records(records)
    assert df["temp_range_c"].iloc[0] == pytest.approx(8.3, abs=0.1)


def test_transform_precipitation_category():
    records = [make_record(precipitation_sum=25.0)]
    df = transform_records(records)
    assert df["precipitation_category"].iloc[0] == "heavy"
    assert df["is_heavy_rain"].iloc[0] is True


def test_transform_heavy_rain_flag():
    df_light = transform_records([make_record(precipitation_sum=3.0)])
    df_heavy = transform_records([make_record(precipitation_sum=25.0)])
    assert df_light["is_heavy_rain"].iloc[0] is False
    assert df_heavy["is_heavy_rain"].iloc[0] is True


def test_transform_high_wind_flag():
    df_calm  = transform_records([make_record(wind_speed_10m_max=20.0)])
    df_windy = transform_records([make_record(wind_speed_10m_max=90.0)])
    assert df_calm["is_high_wind"].iloc[0] is False
    assert df_windy["is_high_wind"].iloc[0] is True


def test_transform_run_id_assigned():
    records = [make_record()]
    df = transform_records(records, run_id="test_run_001")
    assert df["run_id"].iloc[0] == "test_run_001"


def test_transform_city_slug():
    records = [make_record(city="New York")]
    df = transform_records(records)
    assert df["city_slug"].iloc[0] == "new_york"


def test_transform_multiple_cities():
    records = [
        make_record(city="Portland"),
        make_record(city="Seattle", latitude=47.6, longitude=-122.3),
        make_record(city="Denver",  latitude=39.7, longitude=-104.9),
    ]
    df = transform_records(records)
    assert len(df) == 3
    assert set(df["city"]) == {"Portland", "Seattle", "Denver"}


def test_transform_empty_records():
    df = transform_records([])
    assert df.empty


def test_transform_none_values_handled():
    records = [make_record(precipitation_sum=None, wind_speed_10m_max=None)]
    df = transform_records(records)
    assert df["is_heavy_rain"].iloc[0] is False
    assert df["is_high_wind"].iloc[0] is False


def test_transform_anomaly_with_historical():
    # Build historical data where Portland is always ~8°C
    hist_data = [make_record(temperature_2m_mean=8.0 + i*0.1) for i in range(20)]
    hist_df = transform_records(hist_data)

    # Now a 35°C day should be flagged as extreme
    extreme_record = [make_record(temperature_2m_mean=35.0)]
    df = transform_records(extreme_record, historical_df=hist_df)
    assert df["is_extreme_temp"].iloc[0] is True


# ── Parquet serialization ─────────────────────────────────────────────────────

def test_df_to_parquet_bytes_produces_bytes():
    records = [make_record()]
    df = transform_records(records)
    b = df_to_parquet_bytes(df)
    assert isinstance(b, bytes)
    assert len(b) > 0


def test_df_to_parquet_bytes_roundtrip():
    import io
    import pyarrow.parquet as pq

    records = [make_record(), make_record(city="Seattle", latitude=47.6, longitude=-122.3)]
    df = transform_records(records)
    b = df_to_parquet_bytes(df)

    buf = io.BytesIO(b)
    df_back = pq.read_table(buf).to_pandas()
    assert len(df_back) == 2
    assert set(df_back["city"]) == {"Portland", "Seattle"}
