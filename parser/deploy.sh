#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)
SA_NAME="your-service-account-name"
SA="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"
REGION="your-region"

gcloud functions deploy deepseek_parser \
    --runtime=python313 --region="$REGION" \
    --entry-point=deepseek_parser \
    --trigger-topic=receipts.text \
    --service-account="$SA" \
    --env-vars-file .env.deepseek_parser.yaml \
    --set-secrets="DEEPSEEK_API_KEY=DEEPSEEK_API_KEY:latest"