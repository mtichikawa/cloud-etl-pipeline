#!/bin/bash
# infra/deploy_lambda.sh — Package and deploy Lambda to AWS
# Usage: bash infra/deploy_lambda.sh

set -e

FUNCTION_NAME="weather-etl-validator"
REGION=${AWS_REGION:-"us-west-2"}
ROLE_ARN=${LAMBDA_ROLE_ARN:-""}   # Set this env var to your Lambda execution role ARN

if [ -z "$ROLE_ARN" ]; then
    echo "ERROR: Set LAMBDA_ROLE_ARN environment variable to your Lambda execution role ARN"
    echo "       You can create the role manually in AWS Console or with the IAM policy in infra/iam_policy.json"
    exit 1
fi

echo "Packaging Lambda..."
mkdir -p /tmp/lambda_package
pip install boto3 pandas pyarrow -t /tmp/lambda_package --quiet

cp infra/lambda_function.py /tmp/lambda_package/lambda_function.py

cd /tmp/lambda_package
zip -r /tmp/lambda_deployment.zip . -q
cd -

echo "Deploying Lambda function: $FUNCTION_NAME"

# Check if function exists
if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$REGION" 2>/dev/null; then
    # Update existing
    aws lambda update-function-code \
        --function-name "$FUNCTION_NAME" \
        --zip-file fileb:///tmp/lambda_deployment.zip \
        --region "$REGION"
    echo "Lambda updated."
else
    # Create new
    aws lambda create-function \
        --function-name "$FUNCTION_NAME" \
        --runtime python3.11 \
        --role "$ROLE_ARN" \
        --handler lambda_function.lambda_handler \
        --zip-file fileb:///tmp/lambda_deployment.zip \
        --timeout 60 \
        --memory-size 256 \
        --environment "Variables={S3_BUCKET=mtichikawa-weather-etl}" \
        --region "$REGION"
    echo "Lambda created."
fi

echo "Done. Test with:"
echo "  aws lambda invoke --function-name $FUNCTION_NAME --payload '{\"run_id\":\"test\",\"date\":\"2026-02-01\"}' /tmp/response.json"
echo "  cat /tmp/response.json"
