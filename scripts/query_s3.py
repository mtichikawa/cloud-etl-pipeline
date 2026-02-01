"""
scripts/query_s3.py — Query S3 Parquet data with pandas (Athena-lite).

Downloads processed Parquet files from S3 and runs cross-date analyses.
In LOCAL_MODE reads from data/processed/ instead.

Usage:
    python scripts/query_s3.py
    python scripts/query_s3.py --days 30
    LOCAL_MODE=true python scripts/query_s3.py
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
import config
from src.load import S3Loader

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("query_s3")


def load_all_processed(loader: S3Loader, n_days: int = 60) -> pd.DataFrame:
    """Load all available processed Parquet files into a single DataFrame."""
    dates = loader.list_processed_dates()
    if not dates:
        log.warning("No processed dates found. Run backfill.py first.")
        return pd.DataFrame()

    dates = sorted(dates)[-n_days:]   # most recent N days
    log.info(f"Loading {len(dates)} processed files ({dates[0]} → {dates[-1]})")

    frames = []
    for d in dates:
        df = loader.read_processed(d)
        if df is not None:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"])
    log.info(f"Loaded {len(combined):,} rows across {len(frames)} dates")
    return combined


def run_analysis(df: pd.DataFrame) -> None:
    """Print several analytical summaries."""
    print(f"\n{'='*60}")
    print(f"WEATHER ETL — S3 DATA ANALYSIS")
    print(f"{'='*60}")
    print(f"Date range:  {df['date'].min().date()} → {df['date'].max().date()}")
    print(f"Total rows:  {len(df):,}")
    print(f"Cities:      {sorted(df['city'].unique())}")

    # ── City temperature summary ───────────────────────────────────────────────
    print(f"\n--- Average Temperature by City (°C) ---")
    city_temps = (
        df.groupby("city")
        .agg(
            avg_temp=("avg_temp_c", "mean"),
            max_ever=("max_temp_c", "max"),
            min_ever=("min_temp_c", "min"),
            days=("date", "count"),
        )
        .sort_values("avg_temp", ascending=False)
        .round(2)
    )
    print(city_temps.to_string())

    # ── Extreme event summary ──────────────────────────────────────────────────
    print(f"\n--- Extreme Events by City ---")
    extreme = (
        df.groupby("city")
        .agg(
            extreme_temp_days=("is_extreme_temp", "sum"),
            heavy_rain_days=("is_heavy_rain", "sum"),
            high_wind_days=("is_high_wind", "sum"),
        )
        .sort_values("extreme_temp_days", ascending=False)
    )
    print(extreme.to_string())

    # ── Wettest months ─────────────────────────────────────────────────────────
    print(f"\n--- Total Precipitation by Month (mm, summed across all cities) ---")
    df["month"] = df["date"].dt.to_period("M")
    monthly_precip = (
        df.groupby("month")["precipitation_mm"]
        .sum()
        .sort_index()
        .round(1)
    )
    print(monthly_precip.to_string())

    # ── Warmest single day ─────────────────────────────────────────────────────
    if "max_temp_c" in df.columns:
        idx = df["max_temp_c"].idxmax()
        row = df.loc[idx]
        print(f"\n--- Hottest Day in Dataset ---")
        print(f"  City: {row['city']}, Date: {row['date'].date()}, Max Temp: {row['max_temp_c']:.1f}°C")

    # ── Coldest single day ─────────────────────────────────────────────────────
    if "min_temp_c" in df.columns:
        idx = df["min_temp_c"].idxmin()
        row = df.loc[idx]
        print(f"\n--- Coldest Day in Dataset ---")
        print(f"  City: {row['city']}, Date: {row['date'].date()}, Min Temp: {row['min_temp_c']:.1f}°C")

    print(f"\n{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(description="Query processed S3 weather data")
    parser.add_argument("--days", type=int, default=60,
                        help="Number of recent days to include (default: 60)")
    args = parser.parse_args()

    loader = S3Loader()
    df = load_all_processed(loader, n_days=args.days)

    if df.empty:
        print("No data found. Run: python scripts/backfill.py --days 60")
        return

    run_analysis(df)


if __name__ == "__main__":
    main()
