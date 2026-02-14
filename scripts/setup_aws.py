"""
scripts/setup_aws.py — One-time AWS resource provisioning.

Creates:
  - S3 bucket: mtichikawa-weather-etl (with versioning enabled)
  - DynamoDB table: weather-etl-runs (on-demand billing, Free Tier)

Run once before first pipeline execution:
    python scripts/setup_aws.py
"""

import logging
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).parent.parent))
import config

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("setup_aws")


def create_s3_bucket(s3_client, bucket_name: str, region: str) -> bool:
    """Create S3 bucket with versioning and appropriate region config."""
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )

        # Enable versioning
        s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Enabled"},
        )

        # Block public access
        s3_client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls":       True,
                "IgnorePublicAcls":      True,
                "BlockPublicPolicy":     True,
                "RestrictPublicBuckets": True,
            },
        )

        log.info(f"Created S3 bucket: {bucket_name} (region={region})")
        return True

    except ClientError as e:
        if e.response["Error"]["Code"] in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            log.info(f"S3 bucket already exists: {bucket_name}")
            return True
        log.error(f"Failed to create S3 bucket: {e}")
        return False


def create_dynamodb_table(dynamodb_client, table_name: str) -> bool:
    """Create DynamoDB table for pipeline run logging."""
    try:
        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "run_id", "KeyType": "HASH"},
                {"AttributeName": "date",   "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "run_id", "AttributeType": "S"},
                {"AttributeName": "date",   "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",  # on-demand — no provisioned throughput charges
        )
        log.info(f"Created DynamoDB table: {table_name}")
        return True

    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceInUseException":
            log.info(f"DynamoDB table already exists: {table_name}")
            return True
        log.error(f"Failed to create DynamoDB table: {e}")
        return False


def verify_setup(s3_client, dynamodb_client) -> None:
    """Verify both resources are accessible."""
    # S3
    try:
        s3_client.head_bucket(Bucket=config.S3_BUCKET)
        log.info(f"✓ S3 bucket accessible: {config.S3_BUCKET}")
    except ClientError as e:
        log.error(f"✗ S3 bucket not accessible: {e}")

    # DynamoDB
    try:
        resp = dynamodb_client.describe_table(TableName=config.DYNAMODB_TABLE)
        status = resp["Table"]["TableStatus"]
        log.info(f"✓ DynamoDB table accessible: {config.DYNAMODB_TABLE} (status={status})")
    except ClientError as e:
        log.error(f"✗ DynamoDB table not accessible: {e}")


def main():
    log.info("Setting up AWS resources for Cloud ETL Pipeline...")
    log.info(f"  Region: {config.AWS_REGION}")
    log.info(f"  S3 Bucket: {config.S3_BUCKET}")
    log.info(f"  DynamoDB Table: {config.DYNAMODB_TABLE}")

    s3 = boto3.client("s3", region_name=config.AWS_REGION)
    ddb = boto3.client("dynamodb", region_name=config.AWS_REGION)

    s3_ok  = create_s3_bucket(s3, config.S3_BUCKET, config.AWS_REGION)
    ddb_ok = create_dynamodb_table(ddb, config.DYNAMODB_TABLE)

    if s3_ok and ddb_ok:
        verify_setup(s3, ddb)
        log.info("\nSetup complete. You can now run:")
        log.info("  python src/pipeline.py")
        log.info("  python scripts/backfill.py --days 60")
    else:
        log.error("Setup incomplete — check errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()

# create_s3_bucket() with versioning and public access block
