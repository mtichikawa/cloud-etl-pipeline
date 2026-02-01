"""
scripts/backfill.py — Replay historical ETL runs for N prior days.

Simulates 60 days of pipeline history by running the full
extract→transform→load cycle for each past date.

The Open-Meteo archive API supports dates back years — this is real data.

Usage:
    python scripts/backfill.py --days 60
    python scripts/backfill.py --days 60 --start-date 2025-12-01
    LOCAL_MODE=true python scripts/backfill.py --days 10
"""

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))
import config
from src.pipeline import run_pipeline

logging.basicConfig(
    level=logging.WARNING,   # suppress verbose output during backfill
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("backfill")


def backfill(n_days: int, start_date: str = None, skip_existing: bool = True):
    """
    Run the ETL pipeline for each of the last N days.

    Args:
        n_days:       Number of days to backfill
        start_date:   ISO date to start from (default: n_days ago from today)
        skip_existing: Skip dates that already have processed files in S3/local
    """
    from src.load import S3Loader
    loader = S3Loader()

    if start_date:
        end = date.fromisoformat(start_date) + timedelta(days=n_days - 1)
    else:
        end = date.today() - timedelta(days=1)  # yesterday
        start = end - timedelta(days=n_days - 1)

    target_dates = []
    current = end - timedelta(days=n_days - 1)
    while current <= end:
        target_dates.append(current.isoformat())
        current += timedelta(days=1)

    log.warning(f"Backfilling {n_days} days: {target_dates[0]} → {target_dates[-1]}")

    # Check existing if skip_existing
    existing_dates = set()
    if skip_existing:
        existing_dates = set(loader.list_processed_dates())
        if existing_dates:
            log.warning(f"Found {len(existing_dates)} existing processed dates — will skip.")

    results = []
    success = 0
    skipped = 0
    failed  = 0

    with tqdm(target_dates, desc="Backfilling", unit="day") as pbar:
        for target_date in pbar:
            pbar.set_postfix(date=target_date, success=success, failed=failed)

            if target_date in existing_dates:
                skipped += 1
                continue

            try:
                result = run_pipeline(target_date)
                results.append(result)
                if result.get("status") in ("success", "partial"):
                    success += 1
                else:
                    failed += 1
            except Exception as e:
                log.error(f"Pipeline failed for {target_date}: {e}")
                failed += 1

            # Polite delay between days to avoid hammering the API
            time.sleep(0.5)

    print(f"\n{'='*50}")
    print(f"Backfill Complete")
    print(f"{'='*50}")
    print(f"  Total dates:  {n_days}")
    print(f"  Success:      {success}")
    print(f"  Skipped:      {skipped}")
    print(f"  Failed:       {failed}")

    if results:
        import pandas as pd
        df_results = pd.DataFrame(results)
        total_extreme = df_results["extreme_events"].sum() if "extreme_events" in df_results else 0
        print(f"  Total records written: {success * len(config.CITIES)}")
        print(f"  Extreme events found:  {total_extreme}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Backfill ETL pipeline history")
    parser.add_argument("--days",       type=int, default=60,
                        help="Number of days to backfill (default: 60)")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Start date ISO format (default: N days ago from today)")
    parser.add_argument("--no-skip",    action="store_true",
                        help="Re-run even if processed file already exists")
    args = parser.parse_args()

    backfill(
        n_days=args.days,
        start_date=args.start_date,
        skip_existing=not args.no_skip,
    )


if __name__ == "__main__":
    main()
