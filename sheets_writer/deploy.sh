#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)
SA_NAME="your-service-account-name"
SA="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"
REGION="your-region"

gcloud functions deploy sheets_writer_valid \
    --runtime=python313 --region="$REGION" \
    --entry-point=sheets_writer \
    --trigger-topic=receipts.valid \
    --service-account="$SA" \
    --env-vars-file .env.sheets_writer.yaml \
    && gcloud functions deploy sheets_writer_review \
        --runtime=python313 --region="$REGION" \
        --entry-point=sheets_writer \
        --trigger-topic=receipts.review \
        --service-account="$SA" \
        --env-vars-file .env.sheets_writer.yaml