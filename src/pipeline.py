"""
src/pipeline.py — End-to-end ETL pipeline orchestrator.

Runs all three stages (extract → transform → load) for a given date,
then invokes the Lambda validator and logs the run.

Usage:
    python src/pipeline.py                    # runs for today
    python src/pipeline.py --date 2026-02-01  # specific date
    LOCAL_MODE=true python src/pipeline.py    # no AWS calls
"""

import argparse
import json
import logging
import sys
import time
import uuid
from datetime import date, timedelta
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).parent.parent))
import config
from src.extract import extract_all_cities
from src.transform import transform_records
from src.load import S3Loader

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("pipeline")


def invoke_lambda(run_meta: dict) -> dict:
    """
    Invoke the Lambda validator function asynchronously.
    Returns Lambda response or empty dict if unavailable/local mode.
    """
    if config.LOCAL_MODE:
        log.info("LOCAL_MODE: skipping Lambda invocation.")
        return {"status": "skipped (local mode)"}

    try:
        client = boto3.client("lambda", region_name=config.AWS_REGION)
        resp = client.invoke(
            FunctionName=config.LAMBDA_FUNCTION,
            InvocationType="Event",          # async — fire and forget
            Payload=json.dumps(run_meta).encode(),
        )
        status_code = resp.get("StatusCode", 0)
        log.info(f"Lambda invoked: status={status_code}")
        return {"status_code": status_code}
    except ClientError as e:
        log.warning(f"Lambda invocation failed (non-critical): {e}")
        return {"error": str(e)}


def run_pipeline(target_date: str) -> dict:
    """
    Execute the full ETL pipeline for one date.

    Returns run metadata dict with status, counts, URIs, duration.
    """
    run_id = str(uuid.uuid4())[:8]
    t_start = time.perf_counter()
    log.info(f"{'='*60}")
    log.info(f"Pipeline start | run_id={run_id} | date={target_date}")
    log.info(f"{'='*60}")

    loader = S3Loader()

    # ── Stage 1: Extract ───────────────────────────────────────────────────────
    log.info("Stage 1/3: Extract")
    records = extract_all_cities(target_date)

    if not records:
        log.error("Extraction returned no records — aborting pipeline.")
        return {"run_id": run_id, "date": target_date, "status": "failed_extract",
                "cities_extracted": 0, "cities_failed": len(config.CITIES)}

    cities_failed = len(config.CITIES) - len(records)

    # Upload raw to S3
    raw_keys = loader.upload_raw(records, target_date)

    # ── Stage 2: Transform ────────────────────────────────────────────────────
    log.info("Stage 2/3: Transform")

    # Try to load recent historical data for anomaly detection
    historical_df = None
    try:
        frames = []
        for i in range(1, 31):   # last 30 days
            past_date = (
                date.fromisoformat(target_date) - timedelta(days=i)
            ).isoformat()
            df_past = loader.read_processed(past_date)
            if df_past is not None:
                frames.append(df_past)
        if frames:
            historical_df = pd.concat(frames, ignore_index=True)
            log.info(f"Loaded {len(historical_df)} historical rows for anomaly context")
    except Exception as e:
        log.warning(f"Could not load historical data: {e} — proceeding without anomaly flags")

    import pandas as pd
    df = transform_records(records, run_id=run_id, historical_df=historical_df)

    # ── Stage 3: Load ─────────────────────────────────────────────────────────
    log.info("Stage 3/3: Load")
    processed_uri = loader.upload_processed(df, target_date)

    # ── Post-pipeline ─────────────────────────────────────────────────────────
    duration = time.perf_counter() - t_start
    extreme_events = int(df["is_extreme_temp"].sum() + df["is_heavy_rain"].sum() + df["is_high_wind"].sum())

    run_meta = {
        "run_id":            run_id,
        "date":              target_date,
        "cities_extracted":  len(records),
        "cities_failed":     cities_failed,
        "extreme_events":    extreme_events,
        "raw_keys":          raw_keys,
        "processed_uri":     processed_uri or "",
        "duration_seconds":  round(duration, 2),
        "status":            "success" if processed_uri else "partial",
    }

    # Invoke Lambda validator
    lambda_result = invoke_lambda(run_meta)
    run_meta["lambda_result"] = lambda_result

    # Log to DynamoDB
    loader.log_run(run_meta)

    log.info(f"Pipeline complete | run_id={run_id} | {len(records)} cities | "
             f"{extreme_events} extreme events | {duration:.1f}s")

    return run_meta


def main():
    parser = argparse.ArgumentParser(description="Weather ETL Pipeline")
    parser.add_argument("--date", type=str, default=date.today().isoformat(),
                        help="Target date ISO format (default: today)")
    args = parser.parse_args()

    result = run_pipeline(args.date)
    print(f"\nRun summary:")
    print(f"  run_id:           {result['run_id']}")
    print(f"  date:             {result['date']}")
    print(f"  status:           {result['status']}")
    print(f"  cities_extracted: {result.get('cities_extracted', 0)}")
    print(f"  extreme_events:   {result.get('extreme_events', 0)}")
    print(f"  duration:         {result.get('duration_seconds', 0):.1f}s")
    print(f"  processed_uri:    {result.get('processed_uri', 'n/a')}")


if __name__ == "__main__":
    main()

# run_pipeline(): orchestrates extract -> transform -> load
