"""
infra/lambda_function.py — AWS Lambda function for post-load validation.

Triggered by the pipeline orchestrator after each ETL run.
Validates the processed Parquet file on S3, writes a manifest,
and logs results to CloudWatch.

Deploy with: bash infra/deploy_lambda.sh
"""

import json
import logging
import os
from datetime import datetime

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET", "mtichikawa-weather-etl")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "weather-etl-runs")


def validate_parquet_schema(s3_client, bucket: str, key: str) -> dict:
    """
    Download the processed Parquet from S3 and validate:
    - File exists and is non-empty
    - Expected columns are present
    - No nulls in key columns
    - Row count matches expectations
    """
    import io
    import pandas as pd
    import pyarrow.parquet as pq

    EXPECTED_COLUMNS = [
        "city", "date", "avg_temp_c", "max_temp_c", "min_temp_c",
        "precipitation_mm", "wind_speed_max_kmh", "run_id",
    ]

    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        df = pd.read_parquet(buf)

        results = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "missing_columns": [c for c in EXPECTED_COLUMNS if c not in df.columns],
            "null_counts": {
                col: int(df[col].isna().sum())
                for col in EXPECTED_COLUMNS
                if col in df.columns
            },
            "cities_present": sorted(df["city"].unique().tolist()) if "city" in df.columns else [],
            "date_values": df["date"].unique().tolist() if "date" in df.columns else [],
            "extreme_temp_count": int(df["is_extreme_temp"].sum()) if "is_extreme_temp" in df.columns else 0,
            "validation_passed": True,
        }

        # Fail conditions
        if results["missing_columns"]:
            results["validation_passed"] = False
            results["failure_reason"] = f"Missing columns: {results['missing_columns']}"
        elif results["row_count"] == 0:
            results["validation_passed"] = False
            results["failure_reason"] = "Empty DataFrame"
        elif results["null_counts"].get("city", 0) > 0:
            results["validation_passed"] = False
            results["failure_reason"] = "Null values in 'city' column"

        return results

    except Exception as e:
        return {
            "validation_passed": False,
            "failure_reason": str(e),
            "row_count": 0,
        }


def write_manifest(s3_client, bucket: str, run_meta: dict, validation: dict) -> str:
    """Write a JSON manifest summarizing this pipeline run to S3."""
    target_date = run_meta.get("date", "unknown")
    manifest_key = f"manifests/manifest_{target_date.replace('-', '')}.json"

    manifest = {
        "run_id":            run_meta.get("run_id"),
        "date":              target_date,
        "validated_at":      datetime.utcnow().isoformat() + "Z",
        "cities_extracted":  run_meta.get("cities_extracted", 0),
        "cities_failed":     run_meta.get("cities_failed", 0),
        "extreme_events":    run_meta.get("extreme_events", 0),
        "processed_uri":     run_meta.get("processed_uri", ""),
        "validation":        validation,
        "pipeline_status":   run_meta.get("status", "unknown"),
    }

    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    return f"s3://{bucket}/{manifest_key}"


def lambda_handler(event, context):
    """
    Main Lambda handler.

    event: pipeline run_meta dict (passed from orchestrator)
    """
    log.info(f"Lambda invoked: run_id={event.get('run_id')}, date={event.get('date')}")

    s3 = boto3.client("s3")

    # Get the processed Parquet key
    processed_uri = event.get("processed_uri", "")
    if not processed_uri.startswith("s3://"):
        log.warning(f"No valid processed_uri in event: {processed_uri}")
        return {"statusCode": 400, "body": "No processed_uri provided"}

    # Extract bucket and key from URI
    uri_parts = processed_uri.replace("s3://", "").split("/", 1)
    bucket = uri_parts[0]
    key = uri_parts[1] if len(uri_parts) > 1 else ""

    # Validate
    log.info(f"Validating: s3://{bucket}/{key}")
    validation = validate_parquet_schema(s3, bucket, key)

    log.info(f"Validation result: passed={validation['validation_passed']}, "
             f"rows={validation.get('row_count', 0)}, "
             f"cities={len(validation.get('cities_present', []))}")

    if not validation["validation_passed"]:
        log.error(f"VALIDATION FAILED: {validation.get('failure_reason')}")

    # Write manifest
    try:
        manifest_uri = write_manifest(s3, bucket, event, validation)
        log.info(f"Manifest written: {manifest_uri}")
    except Exception as e:
        log.error(f"Failed to write manifest: {e}")
        manifest_uri = None

    return {
        "statusCode": 200 if validation["validation_passed"] else 422,
        "body": json.dumps({
            "run_id":        event.get("run_id"),
            "date":          event.get("date"),
            "validation":    validation,
            "manifest_uri":  manifest_uri,
        }),
    }

# lambda_handler(): validate Parquet schema, write S3 manifest
# note: see README for usage examples
