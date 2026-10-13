# Cloud ETL Pipeline

An end-to-end **AWS cloud data pipeline** that extracts public weather data, transforms it with pandas, loads processed datasets into **S3**, and triggers serverless processing via **AWS Lambda**. Orchestrated locally with boto3 and designed to run on AWS Free Tier.

---

## Motivation

Cloud data pipelines are the backbone of production data engineering. This project bridges the gap between local scripts and real cloud infrastructure — using actual AWS services (S3, Lambda, CloudWatch Logs) with a local orchestrator that mirrors how a production pipeline would be scheduled and monitored.

---

## What This Project Does

1. **Extracts** daily weather summaries from the Open-Meteo API (free, no key required) for 10 US cities
2. **Transforms** raw JSON into clean, enriched Parquet files with derived metrics (heat index, precipitation and wind categories, extreme-weather flags)
3. **Loads** processed files to S3 with a structured `year=/month=/day=` Hive-partitioned prefix
4. **Triggers** an AWS Lambda function that runs lightweight validation and writes a summary manifest
5. **Monitors** pipeline runs via CloudWatch Logs and a run record written to DynamoDB
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
| Testing | pytest, moto (AWS mock) |
| Local orchestration | Python CLI (`src/pipeline.py`) |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Local Orchestrator (pipeline.py)                           │
│                                                             │
│  1. extract.py ──► Open-Meteo API                          │
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
| date | string | Observation date (YYYY-MM-DD) |
| avg_temp_c | float | Daily average temperature (°C) |
| max_temp_c | float | Daily maximum temperature (°C) |
| min_temp_c | float | Daily minimum temperature (°C) |
| precipitation_mm | float | Total precipitation |
| wind_speed_max_kmh | float | Max wind speed (km/h) |
| heat_index | float | Derived: apparent temperature (assumes 50% humidity) |
| precipitation_category | string | none / light / moderate / heavy / extreme |
| wind_category | string | calm / breezy / windy / strong / storm |
| is_extreme_temp | bool | True if avg temp is more than 2σ from the city's prior 30 days |
| is_heavy_rain | bool | True if precipitation ≥ 20 mm |
| is_high_wind | bool | True if max wind ≥ 80 km/h |
| run_id | string | Pipeline run ID for lineage |

Other columns: latitude, longitude, timezone, temp_range_c, wind_gusts_max_kmh, solar_radiation_mj, evapotranspiration_mm, city_slug, processed_at.

---

## Running Without AWS (Local Mode)

Set `LOCAL_MODE=true` in your environment to skip all AWS calls and write outputs locally:

```bash
LOCAL_MODE=true python src/pipeline.py
```

In local mode: raw JSON goes to `data/raw/`, Parquet to `data/processed/`, quarantined rows to `data/quarantine/`, the Lambda invocation is skipped, and the run record is appended to `data/outputs/run_log.jsonl` instead of DynamoDB.

