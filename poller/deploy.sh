#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)
SA_NAME="your-service-account-name"
SA="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"
REGION="your-region"
TIMEZONE="your-timezone-tz"

gcloud functions deploy drive_poller \
    --runtime=python313 --region="$REGION" \
    --entry-point=drive_poller \
    --trigger-http \
    --service-account="$SA" \
    --env-vars-file .env.poller.yaml \
    && URL=(gcloud run functions describe drive_poller --region="$SA" --format='value(serviceConfig.uri)') \
    && gcloud scheduler jobs create http receipts-drive-poller \
        --schedule="* * * * *" --time-zone="$TIMEZONE" \
        --location="$REGION" --http-method=GET \
        --uri="$URL" \
        --oidc-service-account-email="$SA"