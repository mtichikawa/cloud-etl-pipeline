"""
src/transform.py — Data transformation layer.

Converts raw API records into an enriched, analysis-ready DataFrame
with derived metrics, anomaly flags, and AQI categories.
Outputs Parquet format for efficient S3 storage and querying.

Usage:
    from src.transform import transform_records
    df = transform_records(records, run_id="abc123")
"""

import io
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
import config

log = logging.getLogger("transform")

# ── Schema ─────────────────────────────────────────────────────────────────────
OUTPUT_SCHEMA = pa.schema([
    pa.field("city",                    pa.string()),
    pa.field("latitude",                pa.float64()),
    pa.field("longitude",               pa.float64()),
    pa.field("timezone",                pa.string()),
    pa.field("date",                    pa.string()),
    pa.field("avg_temp_c",              pa.float64()),
    pa.field("max_temp_c",              pa.float64()),
    pa.field("min_temp_c",              pa.float64()),
    pa.field("temp_range_c",            pa.float64()),
    pa.field("precipitation_mm",        pa.float64()),
    pa.field("wind_speed_max_kmh",      pa.float64()),
    pa.field("wind_gusts_max_kmh",      pa.float64()),
    pa.field("solar_radiation_mj",      pa.float64()),
    pa.field("evapotranspiration_mm",   pa.float64()),
    pa.field("heat_index",              pa.float64()),
    pa.field("precipitation_category",  pa.string()),
    pa.field("wind_category",           pa.string()),
    pa.field("is_extreme_temp",         pa.bool_()),
    pa.field("is_heavy_rain",           pa.bool_()),
    pa.field("is_high_wind",            pa.bool_()),
    pa.field("city_slug",               pa.string()),
    pa.field("run_id",                  pa.string()),
    pa.field("processed_at",            pa.string()),
])

# ── Heat index calculation ─────────────────────────────────────────────────────
def heat_index_celsius(temp_c: float, humidity_pct: float = 50.0) -> float:
    """
    Rothfusz heat index formula (simplified, assuming 50% humidity).
    Returns apparent temperature in °C.
    """
    if temp_c is None or np.isnan(temp_c):
        return float("nan")
    # Convert to Fahrenheit for formula
    T = temp_c * 9/5 + 32
    RH = humidity_pct
    HI = (-42.379 + 2.04901523*T + 10.14333127*RH
          - 0.22475541*T*RH - 0.00683783*T**2
          - 0.05481717*RH**2 + 0.00122874*T**2*RH
          + 0.00085282*T*RH**2 - 0.00000199*T**2*RH**2)
    # Convert back to Celsius
    return round((HI - 32) * 5/9, 2)


# ── Categorization helpers ─────────────────────────────────────────────────────
def precipitation_category(mm: Optional[float]) -> str:
    if mm is None or np.isnan(mm):
        return "unknown"
    if mm == 0:         return "none"
    if mm < 5:          return "light"
    if mm < 20:         return "moderate"
    if mm < 50:         return "heavy"
    return "extreme"


def wind_category(kmh: Optional[float]) -> str:
    if kmh is None or np.isnan(kmh):
        return "unknown"
    if kmh < 20:   return "calm"
    if kmh < 50:   return "breezy"
    if kmh < 80:   return "windy"
    if kmh < 120:  return "strong"
    return "storm"


# ── Core transform ─────────────────────────────────────────────────────────────
def transform_records(
    records: list[dict],
    run_id: Optional[str] = None,
    historical_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Transform raw extraction records into an enriched DataFrame.

    Args:
        records: List of raw dicts from extract.py
        run_id:  Pipeline run UUID for data lineage
        historical_df: Optional prior data for computing rolling anomaly stats

    Returns:
        Enriched pandas DataFrame matching OUTPUT_SCHEMA
    """
    if not records:
        log.warning("No records to transform.")
        return pd.DataFrame()

    run_id = run_id or str(uuid.uuid4())[:8]
    processed_at = datetime.utcnow().isoformat() + "Z"

    rows = []
    for r in records:
        avg_temp = r.get("temperature_2m_mean")
        max_temp = r.get("temperature_2m_max")
        min_temp = r.get("temperature_2m_min")
        precip   = r.get("precipitation_sum")
        wind_max = r.get("wind_speed_10m_max")
        gusts    = r.get("wind_gusts_10m_max")
        solar    = r.get("shortwave_radiation_sum")
        et0      = r.get("et0_fao_evapotranspiration")

        temp_range = (
            round(max_temp - min_temp, 2)
            if max_temp is not None and min_temp is not None
            else None
        )

        hi = heat_index_celsius(avg_temp) if avg_temp is not None else None

        row = {
            "city":                   r["city"],
            "latitude":               r["latitude"],
            "longitude":              r["longitude"],
            "timezone":               r["timezone"],
            "date":                   r["date"],
            "avg_temp_c":             avg_temp,
            "max_temp_c":             max_temp,
            "min_temp_c":             min_temp,
            "temp_range_c":           temp_range,
            "precipitation_mm":       precip,
            "wind_speed_max_kmh":     wind_max,
            "wind_gusts_max_kmh":     gusts,
            "solar_radiation_mj":     solar,
            "evapotranspiration_mm":  et0,
            "heat_index":             hi,
            "precipitation_category": precipitation_category(precip),
            "wind_category":          wind_category(wind_max),
            "is_extreme_temp":        False,  # updated below if historical data available
            "is_heavy_rain":          precip is not None and precip >= 20,
            "is_high_wind":           wind_max is not None and wind_max >= 80,
            "city_slug":              r["city"].lower().replace(" ", "_"),
            "run_id":                 run_id,
            "processed_at":           processed_at,
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # ── Anomaly detection using historical rolling stats ───────────────────────
    if historical_df is not None and not historical_df.empty:
        for city in df["city"].unique():
            hist_city = historical_df[historical_df["city"] == city]["avg_temp_c"].dropna()
            if len(hist_city) >= 7:
                mean = hist_city.mean()
                std  = hist_city.std()
                mask = df["city"] == city
                df.loc[mask, "is_extreme_temp"] = (
                    (df.loc[mask, "avg_temp_c"] - mean).abs() > 2 * std
                )

    # ── Type enforcement ───────────────────────────────────────────────────────
    for col in ["avg_temp_c", "max_temp_c", "min_temp_c", "temp_range_c",
                "precipitation_mm", "wind_speed_max_kmh", "wind_gusts_max_kmh",
                "solar_radiation_mj", "evapotranspiration_mm", "heat_index"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["is_extreme_temp"] = df["is_extreme_temp"].astype(bool)
    df["is_heavy_rain"]   = df["is_heavy_rain"].astype(bool)
    df["is_high_wind"]    = df["is_high_wind"].astype(bool)

    log.info(f"Transformed {len(df)} records. "
             f"Extreme temps: {df['is_extreme_temp'].sum()}, "
             f"Heavy rain: {df['is_heavy_rain'].sum()}, "
             f"High wind: {df['is_high_wind'].sum()}")

    return df


# ── Serialization ──────────────────────────────────────────────────────────────
def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialize DataFrame to Parquet bytes for S3 upload."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def save_parquet_locally(df: pd.DataFrame, target_date: str) -> Path:
    """Save processed Parquet to data/processed/ for local inspection."""
    compact = target_date.replace("-", "")
    out_path = config.PROCESSED_DIR / f"weather_enriched_{compact}.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path, compression="snappy")
    log.info(f"Saved Parquet locally: {out_path} ({out_path.stat().st_size/1024:.1f} KB)")
    return out_path


def load_parquet_local(path: Path) -> pd.DataFrame:
    """Load a local Parquet file back to DataFrame."""
    return pd.read_parquet(path)


if __name__ == "__main__":
    # Quick smoke test with synthetic data
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    synthetic = [
        {
            "city": "Portland", "latitude": 45.5, "longitude": -122.7,
            "timezone": "America/Los_Angeles", "date": "2026-02-01",
            "temperature_2m_mean": 8.5, "temperature_2m_max": 12.0,
            "temperature_2m_min": 4.0, "precipitation_sum": 5.2,
            "wind_speed_10m_max": 35.0, "wind_gusts_10m_max": 55.0,
            "shortwave_radiation_sum": 4.2, "et0_fao_evapotranspiration": 0.8,
        }
    ]
    df = transform_records(synthetic)
    print(df.T)
