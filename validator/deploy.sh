#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)
SA_NAME="your-service-account-name"
SA="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"
REGION="your-region"

gcloud functions deploy validator \
    --runtime=python313 --"$REGION" \
    --entry-point=validator \
    --trigger-topic=receipts.parsed \
    --service-account="$SA" \
    --env-vars-file .env.validator.yaml