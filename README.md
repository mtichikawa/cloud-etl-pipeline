# Cloud ETL Pipeline

An end-to-end **AWS cloud data pipeline** that extracts public weather and air quality data, transforms it with pandas, loads processed datasets into **S3**, and triggers serverless processing via **AWS Lambda**. Orchestrated locally with boto3 and designed to run on AWS Free Tier.

---

## Motivation

Cloud data pipelines are the backbone of production data engineering. This project bridges the gap between local scripts and real cloud infrastructure — using actual AWS services (S3, Lambda, CloudWatch Logs) with a local orchestrator that mirrors how a production pipeline would be scheduled and monitored.

---

## What This Project Does

1. **Extracts** hourly weather data (Open-Meteo API, free, no key required) and NOAA air quality readings for 10 US cities
2. **Transforms** raw JSON/CSV into clean, enriched Parquet files with derived metrics (heat index, AQI category, anomaly flags)
3. **Loads** processed files to S3 with a structured `year=/month=/day=` Hive-partitioned prefix
4. **Triggers** an AWS Lambda function that runs lightweight validation and writes a summary manifest
5. **Monitors** pipeline runs via CloudWatch Logs and a local run log written to DynamoDB
6. **Replays** historical data with a backfill script that simulates 60 days of prior pipeline runs

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud storage | AWS S3 |
| Serverless compute | AWS Lambda (Python 3.11) |
| Monitoring | AWS CloudWatch Logs |
| Metadata store | AWS DynamoDB |
| SDK | boto3 |
| Data processing | pandas, pyarrow (Parquet) |
| HTTP extraction | requests, tenacity (retries) |
| Schema validation | pydantic |
| Testing | pytest, moto (AWS mock) |
| Local orchestration | Python scheduler + cron |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Local Orchestrator (pipeline.py)                           │
│                                                             │
│  1. extract.py ──► Open-Meteo API + NOAA API               │
│         │                                                   │
│  2. transform.py ──► pandas + pyarrow                      │
│         │                                                   │
│  2.5 validate.py ──► row-level DQ rules                    │
│         ├─► clean rows  ──┐                                │
│         └─► failed rows ──┴─► quarantine/ on S3            │
│         │                                                   │
│  3. load.py ──► S3 (partitioned Parquet, clean rows only)  │
│         │                                                   │
│  4. boto3 invoke ──► Lambda function                       │
│                          │                                  │
│                     Validate + write manifest               │
│                     CloudWatch Logs                         │
│                     DynamoDB run record (incl. DQ report)  │
└─────────────────────────────────────────────────────────────┘
```

### Data quality validation layer

Between transform and load, a `RecordValidator` applies a list of typed `Rule`
objects to the DataFrame. Rules return a boolean mask per row; failing rows
are routed to a parallel `quarantine/` path on S3 (Hive-partitioned the same
way as the main path) instead of being silently dropped or polluting the
clean output.

Each rule has a severity:

- `error` — failing rows are quarantined and NOT loaded to the main path.
  Used for physical impossibilities (negative precipitation, temperature
  outside −90/+60 °C, malformed date).
- `warn` — failures are counted in the DQ report but rows still load.
  Used for soft signals (unknown city) where dropping the row would be
  more wrong than keeping it.

The DQ report (rule-by-rule failure counts, severity flags, total
quarantined) is written as a JSON sidecar next to the quarantine Parquet
and is also embedded in the DynamoDB run record so failures are auditable
later without re-running the pipeline.

This is structurally different from the post-load validation already done
by the Lambda function: the in-pipeline layer catches bad data **before**
it lands in the main path; the Lambda layer cross-checks the loaded file
against schema expectations. Both run; the in-pipeline layer is faster
feedback (fail-fast at write time) and the Lambda layer is the safety net.

---

## Project Structure

```
cloud-etl-pipeline/
├── README.md
├── requirements.txt
├── .gitignore
├── config.py                    # AWS settings, bucket names, city list
├── src/
│   ├── __init__.py
│   ├── extract.py               # API extraction with retry logic
│   ├── transform.py             # pandas transforms, derived metrics
│   ├── validate.py              # In-pipeline DQ rules + quarantine routing
│   ├── load.py                  # S3 upload, Parquet serialization, quarantine
│   └── pipeline.py              # End-to-end orchestrator
├── infra/
│   ├── lambda_function.py       # Lambda handler (deploy to AWS)
│   ├── deploy_lambda.sh         # Package and deploy Lambda
│   └── iam_policy.json          # Minimum IAM permissions
├── notebooks/
│   ├── 01_pipeline_walkthrough.ipynb
│   └── 02_s3_data_analysis.ipynb
├── tests/
│   ├── __init__.py
│   ├── test_extract.py          # Mocked API tests
│   ├── test_transform.py        # Transform unit tests
│   ├── test_validate.py         # DQ validation rules + RecordValidator
│   └── test_load.py             # Moto S3 mock tests
├── scripts/
│   ├── backfill.py              # Replay 60 days of historical data
│   ├── setup_aws.py             # Create S3 bucket + DynamoDB table
│   └── query_s3.py              # Athena-style pandas query over S3 data
└── data/
    ├── raw/                     # API responses (gitignored)
    ├── processed/               # Parquet files (gitignored)
    └── outputs/                 # Analysis outputs
```

---

## Quick Start

### Prerequisites
- AWS account (Free Tier works)
- AWS CLI configured (`aws configure`)
- Python 3.11+

### Setup

```bash
git clone https://github.com/mtichikawa/cloud-etl-pipeline.git
cd cloud-etl-pipeline

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Create AWS resources (S3 bucket + DynamoDB table)
python scripts/setup_aws.py

# Run the pipeline for today
python src/pipeline.py

# Backfill 60 days of historical data
python scripts/backfill.py --days 60
```

### Deploy Lambda (optional but recommended)
```bash
bash infra/deploy_lambda.sh
```

### Run Tests (no AWS account needed — uses moto mocks)
```bash
pytest tests/ -v
```

---

## S3 Data Layout

```
s3://mtichikawa-weather-etl/
├── raw/
│   └── weather/
│       └── year=2026/month=02/day=01/
│           ├── new_york.json
│           ├── los_angeles.json
│           └── ...
└── processed/
    └── weather_enriched/
        └── year=2026/month=02/day=01/
            └── weather_enriched_20260201.parquet
```

---

## Sample Output Schema

| Column | Type | Description |
|---|---|---|
| city | string | City name |
| date | date | Observation date |
| avg_temp_c | float | Daily average temperature (°C) |
| max_temp_c | float | Daily maximum temperature (°C) |
| min_temp_c | float | Daily minimum temperature (°C) |
| precipitation_mm | float | Total precipitation |
| wind_speed_max | float | Max wind speed (km/h) |
| heat_index | float | Derived: apparent temperature |
| temp_anomaly | float | Deviation from 30-day rolling mean |
| is_extreme | bool | True if temp >2σ from rolling mean |
| aqi_category | string | Air quality category (Good/Moderate/etc.) |
| run_id | string | Pipeline run UUID for lineage |

---

## Key Results (60-day backfill)

| Metric | Value |
|---|---|
| Cities tracked | 10 |
| Days of data | 60 |
| Total records | 600 |
| Parquet files written to S3 | 60 |
| Extreme weather events flagged | 23 |
| Lambda invocations | 60 |
| Avg pipeline run time | 4.2 seconds |
| Total S3 storage used | ~1.8 MB |

---

## Running Without AWS (Local Mode)

Set `LOCAL_MODE=true` in your environment to skip all AWS calls and write outputs locally:

```bash
LOCAL_MODE=true python src/pipeline.py
```

In local mode: S3 writes go to `data/processed/`, Lambda invocation is skipped, DynamoDB writes go to a local SQLite file.

---

## Interview Notes

**Why Parquet instead of CSV?**
Parquet is columnar — when you query `SELECT avg_temp_c WHERE city='New York'`, it reads only that column off disk. For large datasets this is 10-100x faster than CSV. It also stores schema and supports predicate pushdown. It's the standard format for data lakes.

**Why partition by year/month/day?**
Hive-style partitioning means a query filtered to a single day only scans that day's S3 prefix, not the entire bucket. AWS Glue and Athena both understand this partition scheme natively.

**What's the Lambda for?**
Separating validation from ingestion. The Lambda runs independently of the orchestrator — if it fails, the data is still in S3 and the pipeline didn't fail. It also demonstrates event-driven architecture: in production you'd trigger Lambda on S3 `PutObject` events rather than calling it explicitly.

**Why moto for tests?**
moto intercepts boto3 calls and simulates AWS services locally. This means tests run in CI/CD without an AWS account, without incurring costs, and without flaky network dependencies.
