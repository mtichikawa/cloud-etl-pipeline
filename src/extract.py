"""
src/extract.py — Data extraction from Open-Meteo weather API.

Uses tenacity for automatic retry with exponential backoff.
Supports both current-day and historical date extraction.

Usage:
    from src.extract import extract_all_cities
    records = extract_all_cities("2026-02-01")
"""

import json
import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
import config

log = logging.getLogger("extract")


# ── Retry decorator ────────────────────────────────────────────────────────────
@retry(
    retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(4),
    reraise=True,
)
def _get_with_retry(url: str, params: dict, timeout: int = 15) -> dict:
    """
    GET request with exponential backoff retry on network errors.

    Args:
        url:     Full URL to request.
        params:  Query parameters dict passed to requests.get.
        timeout: Per-request timeout in seconds (default 15).

    Returns:
        Parsed JSON response as dict.
    """
    resp = requests.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


# ── Extraction functions ───────────────────────────────────────────────────────

def extract_city_weather(
    city_name: str,
    lat: float,
    lon: float,
    timezone: str,
    target_date: str,
) -> Optional[dict]:
    """
    Fetch daily weather summary for one city on one date.

    Uses Open-Meteo archive API for historical dates,
    forecast API for today/tomorrow.

    Returns dict with raw API response + metadata, or None on failure.
    """
    today = date.today().isoformat()
    is_historical = target_date < today

    url = (config.OPEN_METEO_HISTORICAL_URL if is_historical
           else config.OPEN_METEO_URL)

    params = {
        "latitude":       lat,
        "longitude":      lon,
        "daily":          ",".join(config.WEATHER_VARIABLES),
        "timezone":       timezone,
        "start_date":     target_date,
        "end_date":       target_date,
        "temperature_unit": "celsius",
        "wind_speed_unit":  "kmh",
        "precipitation_unit": "mm",
    }

    try:
        t0 = time.perf_counter()
        data = _get_with_retry(url, params)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        # Flatten daily arrays (each is a 1-element list for single-day requests)
        daily = data.get("daily", {})
        record = {
            "city":       city_name,
            "latitude":   lat,
            "longitude":  lon,
            "timezone":   timezone,
            "date":       target_date,
            "source_url": url,
            "fetch_ms":   round(elapsed_ms, 1),
            "fetched_at": datetime.utcnow().isoformat() + "Z",
        }

        for var in config.WEATHER_VARIABLES:
            values = daily.get(var, [None])
            record[var] = values[0] if values else None

        log.debug(f"  {city_name}: fetched in {elapsed_ms:.0f}ms")
        return record

    except requests.HTTPError as e:
        log.warning(f"HTTP error for {city_name} on {target_date}: {e}")
        return None
    except Exception as e:
        log.warning(f"Failed to fetch {city_name} on {target_date}: {e}")
        return None


def extract_all_cities(target_date: str) -> list[dict]:
    """
    Extract weather data for all configured cities on target_date.

    Args:
        target_date: ISO date string, e.g. '2026-02-01'

    Returns:
        List of raw weather records (one per city, failed cities omitted).
    """
    log.info(f"Extracting weather for {len(config.CITIES)} cities on {target_date}...")
    records = []

    for city_name, lat, lon, tz in config.CITIES:
        record = extract_city_weather(city_name, lat, lon, tz, target_date)
        if record:
            records.append(record)
        time.sleep(0.15)  # polite rate limiting — Open-Meteo allows ~10k req/day free

    log.info(f"Extracted {len(records)}/{len(config.CITIES)} cities successfully.")
    return records


def save_raw_locally(records: list[dict], target_date: str) -> list[Path]:
    """Save raw JSON records to data/raw/ for local inspection."""
    paths = []
    date_dir = config.RAW_DIR / target_date.replace("-", "/")
    date_dir.mkdir(parents=True, exist_ok=True)

    for record in records:
        city_slug = record["city"].lower().replace(" ", "_")
        path = date_dir / f"{city_slug}.json"
        with open(path, "w") as f:
            json.dump(record, f, indent=2)
        paths.append(path)

    log.info(f"Saved {len(paths)} raw files to {date_dir}")
    return paths


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    target = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
    records = extract_all_cities(target)
    save_raw_locally(records, target)
    print(f"\nExtracted {len(records)} cities for {target}")
    for r in records:
        print(f"  {r['city']:<20} temp_mean={r.get('temperature_2m_mean')}°C  "
              f"precip={r.get('precipitation_sum')}mm")

# extract_city_weather(): Open-Meteo archive API with tenacity retry

# extract_all_cities(): parallel fetch for all 10 cities
