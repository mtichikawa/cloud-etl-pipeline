"""
src/load.py — Load processed data to AWS S3 and log runs to DynamoDB.

Supports both live AWS mode and LOCAL_MODE (writes to disk, skips AWS).

Usage:
    from src.load import S3Loader
    loader = S3Loader()
    loader.upload_raw(records, target_date)
    loader.upload_processed(df, target_date)
    loader.log_run(run_meta)
"""

import io
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import boto3
import pandas as pd
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
import config
from src.transform import df_to_parquet_bytes, save_parquet_locally

log = logging.getLogger("load")


class S3Loader:
    """
    Handles all AWS I/O: S3 uploads (raw JSON + processed Parquet)
    and DynamoDB run logging.

    In LOCAL_MODE, writes to data/ directories instead of AWS.
    """

    def __init__(self):
        self.local_mode = config.LOCAL_MODE
        if not self.local_mode:
            self.s3       = boto3.client("s3",       region_name=config.AWS_REGION)
            self.dynamodb = boto3.resource("dynamodb", region_name=config.AWS_REGION)
            self.table    = self.dynamodb.Table(config.DYNAMODB_TABLE)
            log.info(f"S3Loader initialized (AWS mode, bucket={config.S3_BUCKET})")
        else:
            self.s3 = self.dynamodb = self.table = None
            log.info("S3Loader initialized (LOCAL_MODE — no AWS calls)")

    # ── S3 raw upload ──────────────────────────────────────────────────────────

    def upload_raw(self, records: list[dict], target_date: str) -> list[str]:
        """
        Upload raw JSON records to S3 raw prefix.

        Returns list of S3 keys (or local paths in LOCAL_MODE).
        """
        keys = []
        for record in records:
            city_slug = record["city"].lower().replace(" ", "_")
            key = config.s3_raw_key(city_slug, target_date)

            raw_bytes = json.dumps(record, indent=2).encode("utf-8")

            if self.local_mode:
                local_path = config.RAW_DIR / key.replace("raw/weather/", "")
                local_path.parent.mkdir(parents=True, exist_ok=True)
                local_path.write_bytes(raw_bytes)
                keys.append(str(local_path))
            else:
                try:
                    self.s3.put_object(
                        Bucket=config.S3_BUCKET,
                        Key=key,
                        Body=raw_bytes,
                        ContentType="application/json",
                        Metadata={"city": record["city"], "date": target_date},
                    )
                    keys.append(f"s3://{config.S3_BUCKET}/{key}")
                    log.debug(f"  Uploaded raw: {key}")
                except ClientError as e:
                    log.error(f"Failed to upload raw for {record['city']}: {e}")

        log.info(f"Uploaded {len(keys)} raw files for {target_date}")
        return keys

    # ── S3 processed upload ────────────────────────────────────────────────────

    def upload_processed(self, df: pd.DataFrame, target_date: str) -> Optional[str]:
        """
        Serialize DataFrame to Parquet and upload to S3 processed prefix.

        Returns S3 URI or local path.
        """
        if df.empty:
            log.warning("Empty DataFrame — skipping processed upload.")
            return None

        key = config.s3_processed_key(target_date)

        if self.local_mode:
            path = save_parquet_locally(df, target_date)
            return str(path)

        try:
            parquet_bytes = df_to_parquet_bytes(df)
            self.s3.put_object(
                Bucket=config.S3_BUCKET,
                Key=key,
                Body=parquet_bytes,
                ContentType="application/octet-stream",
                Metadata={
                    "date":      target_date,
                    "row_count": str(len(df)),
                    "cities":    ",".join(df["city"].unique()),
                },
            )
            uri = f"s3://{config.S3_BUCKET}/{key}"
            log.info(f"Uploaded processed Parquet: {uri} ({len(parquet_bytes)/1024:.1f} KB, {len(df)} rows)")
            return uri
        except ClientError as e:
            log.error(f"Failed to upload processed Parquet: {e}")
            return None

    # ── DynamoDB run logging ───────────────────────────────────────────────────

    def log_run(self, run_meta: dict) -> bool:
        """
        Write pipeline run metadata to DynamoDB (or local JSON in LOCAL_MODE).

        run_meta keys: run_id, date, cities_extracted, cities_failed,
                       extreme_events, raw_keys, processed_uri,
                       duration_seconds, status
        """
        item = {
            "run_id":            run_meta.get("run_id", "unknown"),
            "date":              run_meta.get("date", ""),
            "logged_at":         datetime.utcnow().isoformat() + "Z",
            "cities_extracted":  run_meta.get("cities_extracted", 0),
            "cities_failed":     run_meta.get("cities_failed", 0),
            "extreme_events":    run_meta.get("extreme_events", 0),
            "processed_uri":     run_meta.get("processed_uri", ""),
            "duration_seconds":  str(round(run_meta.get("duration_seconds", 0), 2)),
            "status":            run_meta.get("status", "unknown"),
        }

        if self.local_mode:
            log_path = config.OUTPUTS_DIR / "run_log.jsonl"
            with open(log_path, "a") as f:
                f.write(json.dumps(item) + "\n")
            log.info(f"Run logged locally: {log_path}")
            return True

        try:
            self.table.put_item(Item=item)
            log.info(f"Run logged to DynamoDB: run_id={item['run_id']}")
            return True
        except ClientError as e:
            log.error(f"DynamoDB log failed: {e}")
            return False

    # ── S3 read-back ───────────────────────────────────────────────────────────

    def read_processed(self, target_date: str) -> Optional[pd.DataFrame]:
        """Download and read a processed Parquet file from S3."""
        import io
        import pyarrow.parquet as pq

        key = config.s3_processed_key(target_date)

        if self.local_mode:
            compact = target_date.replace("-", "")
            path = config.PROCESSED_DIR / f"weather_enriched_{compact}.parquet"
            if path.exists():
                return pd.read_parquet(path)
            return None

        try:
            obj = self.s3.get_object(Bucket=config.S3_BUCKET, Key=key)
            buf = io.BytesIO(obj["Body"].read())
            return pd.read_parquet(buf)
        except ClientError as e:
            log.warning(f"Could not read processed file for {target_date}: {e}")
            return None

    def list_processed_dates(self) -> list[str]:
        """List all dates that have processed Parquet files in S3."""
        if self.local_mode:
            files = list(config.PROCESSED_DIR.glob("weather_enriched_*.parquet"))
            dates = []
            for f in sorted(files):
                compact = f.stem.replace("weather_enriched_", "")
                if len(compact) == 8:
                    dates.append(f"{compact[:4]}-{compact[4:6]}-{compact[6:]}")
            return dates

        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket=config.S3_BUCKET,
                Prefix="processed/weather_enriched/",
            )
            dates = []
            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(".parquet"):
                        # Extract date from key: .../day=01/weather_enriched_20260201.parquet
                        fname = key.split("/")[-1]
                        compact = fname.replace("weather_enriched_", "").replace(".parquet", "")
                        if len(compact) == 8:
                            dates.append(f"{compact[:4]}-{compact[4:6]}-{compact[6:]}")
            return sorted(dates)
        except ClientError as e:
            log.error(f"Could not list processed dates: {e}")
            return []

# S3Loader class: upload_raw() with Hive-partitioned keys

# upload_processed(): Parquet to S3 + DynamoDB run logging

# LOCAL_MODE: skip all AWS calls, write to data/ directories
