#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)
SA_NAME="your-service-account-name"
SA="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"
REGION="your-region"

gcloud functions deploy ingress \
  --runtime=python313 --region="$REGION" \
  --entry-point=ingress --trigger-http --allow-unauthenticated \
  --service-account="$SA" \
  --env-vars-file .env.ingress.yaml \
  --set-secrets="API_KEY=RECEIPTS_INGRESS_API_KEY:latest"