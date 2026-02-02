"""
config.py — Configuration for the Cloud ETL Pipeline.

AWS credentials come from environment variables or ~/.aws/credentials.
Set LOCAL_MODE=true to skip all AWS calls and write locally instead.
"""

import os
import uuid
from pathlib import Path

# ── Project root ───────────────────────────────────────────────────────────────
ROOT_DIR      = Path(__file__).parent
DATA_DIR      = ROOT_DIR / "data"
RAW_DIR       = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUTS_DIR   = DATA_DIR / "outputs"

for d in [RAW_DIR, PROCESSED_DIR, OUTPUTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── AWS ────────────────────────────────────────────────────────────────────────
AWS_REGION        = os.getenv("AWS_REGION", "us-west-2")
S3_BUCKET         = os.getenv("S3_BUCKET", "mtichikawa-weather-etl")
DYNAMODB_TABLE    = os.getenv("DYNAMODB_TABLE", "weather-etl-runs")
LAMBDA_FUNCTION   = os.getenv("LAMBDA_FUNCTION", "weather-etl-validator")

# ── Local mode (no AWS calls) ──────────────────────────────────────────────────
LOCAL_MODE = os.getenv("LOCAL_MODE", "false").lower() == "true"

# ── Cities to track ────────────────────────────────────────────────────────────
# (name, latitude, longitude, timezone)
CITIES = [
    ("New York",      40.7128,  -74.0060, "America/New_York"),
    ("Los Angeles",   34.0522, -118.2437, "America/Los_Angeles"),
    ("Chicago",       41.8781,  -87.6298, "America/Chicago"),
    ("Houston",       29.7604,  -95.3698, "America/Chicago"),
    ("Phoenix",       33.4484, -112.0740, "America/Phoenix"),
    ("Philadelphia",  39.9526,  -75.1652, "America/New_York"),
    ("San Antonio",   29.4241,  -98.4936, "America/Chicago"),
    ("San Diego",     32.7157, -117.1611, "America/Los_Angeles"),
    ("Dallas",        32.7767,  -96.7970, "America/Chicago"),
    ("Portland",      45.5051, -122.6750, "America/Los_Angeles"),
]

CITY_NAMES = [c[0] for c in CITIES]

# ── Open-Meteo API (free, no key required) ─────────────────────────────────────
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_HISTORICAL_URL = "https://archive-api.open-meteo.com/v1/archive"

WEATHER_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "shortwave_radiation_sum",
    "et0_fao_evapotranspiration",
]

# ── S3 key patterns ────────────────────────────────────────────────────────────
def s3_raw_key(city_slug: str, date_str: str) -> str:
    """e.g. raw/weather/year=2026/month=02/day=01/new_york.json"""
    y, m, d = date_str[:4], date_str[5:7], date_str[8:10]
    return f"raw/weather/year={y}/month={m}/day={d}/{city_slug}.json"

def s3_processed_key(date_str: str) -> str:
    """e.g. processed/weather_enriched/year=2026/month=02/day=01/weather_enriched_20260201.parquet"""
    y, m, d = date_str[:4], date_str[5:7], date_str[8:10]
    compact = date_str.replace("-", "")
    return f"processed/weather_enriched/year={y}/month={m}/day={d}/weather_enriched_{compact}.parquet"

def s3_manifest_key(date_str: str) -> str:
    compact = date_str.replace("-", "")
    return f"manifests/manifest_{compact}.json"

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Cities list: 10 US cities with lat/lon/timezone
