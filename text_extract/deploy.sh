#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)
SA_NAME="your-service-account-name"
SA="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"
REGION="your-region"

gcloud functions deploy text_extraction \
    --runtime=python313 --region="$REGION" \
    --entry-point=text_extraction \
    --trigger-topic=receipts.new \
    --service-account="$SA" \
    --env-vars-file .env.text_extraction.yaml
