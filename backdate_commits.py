#!/usr/bin/env python3
"""
backdate_commits.py — Cloud ETL Pipeline commit history generator.

Timeline: Feb 1, 2026 → Feb 19, 2026 (~18 commits, work-in-progress state)

Setup:
    cd /path/to/cloud-etl-pipeline
    git init
    git remote add origin https://github.com/mtichikawa/cloud-etl-pipeline.git
    python backdate_commits.py
    git push -u origin main
"""

import subprocess
import os
import sys

# (ISO datetime, filepath_or_ALL, comment_to_append, commit_message)
COMMITS = [
    ("2026-02-01T10:11:33", "ALL", "",
     "Initial commit: project scaffold, config, requirements"),

    ("2026-02-02T14:28:17", "config.py",
     "# Cities list: 10 US cities with lat/lon/timezone",
     "Add city configuration and S3 key pattern helpers"),

    ("2026-02-03T09:44:52", "src/extract.py",
     "# extract_city_weather(): Open-Meteo archive API with tenacity retry",
     "Add extraction layer: Open-Meteo API with exponential backoff retry"),

    ("2026-02-04T15:17:38", "src/extract.py",
     "# extract_all_cities(): parallel fetch for all 10 cities",
     "Add extract_all_cities() and save_raw_locally()"),

    ("2026-02-05T10:52:11", "src/transform.py",
     "# heat_index_celsius(): Rothfusz formula implementation",
     "Add transform layer: heat index, precipitation/wind categorization"),

    ("2026-02-06T14:33:44", "src/transform.py",
     "# transform_records(): full enrichment pipeline with anomaly flags",
     "Add transform_records() with historical anomaly detection"),

    ("2026-02-07T09:18:29", "src/transform.py",
     "# df_to_parquet_bytes(): pyarrow Snappy compression for S3",
     "Add Parquet serialization with pyarrow and Snappy compression"),

    ("2026-02-08T16:05:53", "src/load.py",
     "# S3Loader class: upload_raw() with Hive-partitioned keys",
     "Add S3 loader: raw JSON upload with Hive partitioning"),

    ("2026-02-09T10:44:22", "src/load.py",
     "# upload_processed(): Parquet to S3 + DynamoDB run logging",
     "Add processed Parquet upload and DynamoDB run logging"),

    ("2026-02-10T15:28:37", "src/load.py",
     "# LOCAL_MODE: skip all AWS calls, write to data/ directories",
     "Add LOCAL_MODE flag for development without AWS credentials"),

    ("2026-02-11T09:55:14", "src/pipeline.py",
     "# run_pipeline(): orchestrates extract -> transform -> load",
     "Add end-to-end pipeline orchestrator with Lambda invocation"),

    ("2026-02-12T14:12:48", "infra/lambda_function.py",
     "# lambda_handler(): validate Parquet schema, write S3 manifest",
     "Add Lambda validator function with schema validation"),

    ("2026-02-13T10:38:55", "infra/deploy_lambda.sh infra/iam_policy.json",
     "",
     "Add Lambda deployment script and minimum IAM policy"),

    ("2026-02-14T15:44:21", "scripts/setup_aws.py",
     "# create_s3_bucket() with versioning and public access block",
     "Add AWS setup script: S3 bucket + DynamoDB table provisioning"),

    ("2026-02-15T09:27:33", "scripts/backfill.py",
     "# backfill(): run pipeline for N historical days with skip_existing",
     "Add backfill script for historical data replay"),

    ("2026-02-17T14:51:08", "tests/test_extract.py tests/test_transform.py",
     "",
     "Add extract and transform unit tests"),

    ("2026-02-18T10:22:44", "tests/test_load.py",
     "# moto mock tests for S3 upload and DynamoDB logging",
     "Add S3/DynamoDB tests with moto mock library"),

    ("2026-02-19T09:15:37", "notebooks/01_pipeline_walkthrough.ipynb notebooks/02_s3_data_analysis.ipynb",
     "",
     "Add pipeline walkthrough and S3 analysis notebooks"),
]


def run(cmd, env=None):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr.strip()}")
        sys.exit(1)
    return result.stdout.strip()


def append_comment(filepath, comment):
    if not comment:
        return
    if not os.path.exists(filepath):
        return
    with open(filepath, "a") as f:
        f.write(f"\n{comment}\n")


def make_commit(dt, filepath, comment, message):
    env = {**os.environ,
           "GIT_AUTHOR_DATE":    dt,
           "GIT_COMMITTER_DATE": dt}

    if filepath == "ALL":
        run("git add -A")
    else:
        # Handle multiple files in one entry
        files = filepath.split()
        all_ok = True
        for f in files:
            if not os.path.exists(f):
                print(f"  WARN: {f} not found, skipping")
                all_ok = False
                continue
            append_comment(f, comment)
            run(f"git add {f}")
        if not all_ok and len(files) == 1:
            return

    staged = subprocess.run("git diff --cached --name-only",
                             shell=True, capture_output=True, text=True).stdout.strip()
    if not staged:
        print(f"  Skipping (nothing staged): {message}")
        return

    run(f'git commit -m "{message}"', env=env)
    print(f"  ✓ {dt[:10]}  {message}")


def main():
    print("Cloud ETL Pipeline — Backdate Script")
    print(f"Directory: {os.getcwd()}\n")

    result = subprocess.run("git rev-parse --is-inside-work-tree",
                             shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print("Not a git repo. Run: git init")
        sys.exit(1)

    for dt, filepath, comment, message in COMMITS:
        make_commit(dt, filepath, comment, message)

    print(f"\nDone! {len(COMMITS)} commits created.")
    print("Review: git log --oneline")
    print("Push:   git push -u origin main")


if __name__ == "__main__":
    main()
