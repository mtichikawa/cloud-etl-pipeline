"""
tests/test_load.py — S3 and DynamoDB tests using moto mocks.

moto intercepts boto3 calls and simulates AWS — no real AWS account needed.
Tests run entirely in-process, no network required.
"""

import json
import os
import pytest
import pandas as pd

# Must set fake AWS creds before moto imports
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture(scope="function")
def aws_setup():
    """Create mocked S3 bucket and DynamoDB table for each test."""
    from moto import mock_aws
    import boto3
    import config

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=config.S3_BUCKET)

        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        ddb.create_table(
            TableName=config.DYNAMODB_TABLE,
            KeySchema=[
                {"AttributeName": "run_id", "KeyType": "HASH"},
                {"AttributeName": "date",   "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "run_id", "AttributeType": "S"},
                {"AttributeName": "date",   "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield s3, ddb


@pytest.fixture
def sample_df():
    """Minimal enriched DataFrame for upload tests."""
    from src.transform import transform_records
    records = [
        {
            "city": "Portland", "latitude": 45.5, "longitude": -122.7,
            "timezone": "America/Los_Angeles", "date": "2026-02-01",
            "temperature_2m_mean": 8.1, "temperature_2m_max": 12.5,
            "temperature_2m_min": 4.2, "precipitation_sum": 3.2,
            "wind_speed_10m_max": 28.0, "wind_gusts_10m_max": 45.0,
            "shortwave_radiation_sum": 5.1, "et0_fao_evapotranspiration": 0.9,
        }
    ]
    return transform_records(records)


@pytest.fixture
def sample_records():
    return [
        {
            "city": "Portland", "latitude": 45.5, "longitude": -122.7,
            "timezone": "America/Los_Angeles", "date": "2026-02-01",
            "temperature_2m_mean": 8.1, "temperature_2m_max": 12.5,
            "temperature_2m_min": 4.2, "precipitation_sum": 3.2,
            "wind_speed_10m_max": 28.0, "wind_gusts_10m_max": 45.0,
            "shortwave_radiation_sum": 5.1, "et0_fao_evapotranspiration": 0.9,
        }
    ]


# ── S3 upload tests ────────────────────────────────────────────────────────────

def test_upload_raw_to_s3(aws_setup, sample_records):
    from moto import mock_aws
    import boto3, config
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=config.S3_BUCKET)
        from src.load import S3Loader
        loader = S3Loader()
        loader.local_mode = False
        loader.s3 = boto3.client("s3", region_name="us-east-1")

        keys = loader.upload_raw(sample_records, "2026-02-01")
        assert len(keys) == 1
        assert "s3://" in keys[0]
        assert "portland" in keys[0]


def test_upload_processed_to_s3(aws_setup, sample_df):
    from moto import mock_aws
    import boto3, config
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=config.S3_BUCKET)
        from src.load import S3Loader
        loader = S3Loader()
        loader.local_mode = False
        loader.s3 = boto3.client("s3", region_name="us-east-1")

        uri = loader.upload_processed(sample_df, "2026-02-01")
        assert uri is not None
        assert "s3://" in uri
        assert "weather_enriched_20260201.parquet" in uri


def test_s3_key_structure(aws_setup, sample_records):
    from moto import mock_aws
    import boto3, config
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=config.S3_BUCKET)
        from src.load import S3Loader
        loader = S3Loader()
        loader.local_mode = False
        loader.s3 = s3

        loader.upload_raw(sample_records, "2026-02-01")
        # List objects and verify Hive partitioning
        objs = s3.list_objects_v2(Bucket=config.S3_BUCKET, Prefix="raw/weather/year=2026/month=02/day=01/")
        keys = [o["Key"] for o in objs.get("Contents", [])]
        assert any("portland.json" in k for k in keys)


def test_upload_empty_df_returns_none(aws_setup):
    from moto import mock_aws
    import boto3, config
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=config.S3_BUCKET)
        from src.load import S3Loader
        loader = S3Loader()
        loader.local_mode = False
        loader.s3 = boto3.client("s3", region_name="us-east-1")

        uri = loader.upload_processed(pd.DataFrame(), "2026-02-01")
        assert uri is None


# ── DynamoDB tests ─────────────────────────────────────────────────────────────

def test_log_run_to_dynamodb():
    from moto import mock_aws
    import boto3, config
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=config.S3_BUCKET)
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        ddb.create_table(
            TableName=config.DYNAMODB_TABLE,
            KeySchema=[
                {"AttributeName": "run_id", "KeyType": "HASH"},
                {"AttributeName": "date",   "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "run_id", "AttributeType": "S"},
                {"AttributeName": "date",   "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        from src.load import S3Loader
        loader = S3Loader()
        loader.local_mode = False
        loader.s3 = boto3.client("s3", region_name="us-east-1")
        loader.dynamodb = ddb
        loader.table = ddb.Table(config.DYNAMODB_TABLE)

        run_meta = {
            "run_id": "test_run_001",
            "date": "2026-02-01",
            "cities_extracted": 10,
            "cities_failed": 0,
            "extreme_events": 2,
            "processed_uri": "s3://bucket/key",
            "duration_seconds": 4.5,
            "status": "success",
        }
        result = loader.log_run(run_meta)
        assert result is True

        # Verify item in DynamoDB
        table = ddb.Table(config.DYNAMODB_TABLE)
        item = table.get_item(Key={"run_id": "test_run_001", "date": "2026-02-01"})
        assert "Item" in item
        assert item["Item"]["status"] == "success"
        assert item["Item"]["cities_extracted"] == 10


# ── Local mode tests ───────────────────────────────────────────────────────────

def test_local_mode_upload_writes_to_disk(tmp_path, sample_df, monkeypatch):
    import config
    monkeypatch.setenv("LOCAL_MODE", "true")
    monkeypatch.setattr(config, "LOCAL_MODE", True)
    monkeypatch.setattr(config, "PROCESSED_DIR", tmp_path)

    from src.load import S3Loader
    loader = S3Loader()
    assert loader.local_mode is True

    uri = loader.upload_processed(sample_df, "2026-02-01")
    assert uri is not None
    assert (tmp_path / "weather_enriched_20260201.parquet").exists()


def test_local_mode_log_run_writes_jsonl(tmp_path, monkeypatch):
    import config
    monkeypatch.setattr(config, "LOCAL_MODE", True)
    monkeypatch.setattr(config, "OUTPUTS_DIR", tmp_path)

    from src.load import S3Loader
    loader = S3Loader()

    loader.log_run({
        "run_id": "local_run_001", "date": "2026-02-01",
        "cities_extracted": 5, "cities_failed": 0,
        "extreme_events": 1, "processed_uri": "/local/path",
        "duration_seconds": 2.1, "status": "success",
    })

    log_file = tmp_path / "run_log.jsonl"
    assert log_file.exists()
    line = json.loads(log_file.read_text().strip())
    assert line["run_id"] == "local_run_001"

# moto mock tests for S3 upload and DynamoDB logging
# reviewed: logic verified
