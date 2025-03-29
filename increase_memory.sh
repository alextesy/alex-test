#!/bin/bash

# Script to update Cloud Run job with increased memory and CPU
set -e

# Get project ID and region from environment or use defaults
PROJECT_ID=${GOOGLE_CLOUD_PROJECT_ID:-alex-stocks}
REGION=${GCP_REGION:-us-central1}
JOB_NAME="reddit-etl-job"

echo "Updating Cloud Run job configuration for $JOB_NAME"
echo "Project: $PROJECT_ID"
echo "Region: $REGION"

# Update the Cloud Run job configuration
gcloud run jobs update $JOB_NAME \
  --cpu 8 \
  --memory 16Gi \
  --timeout 3600s \
  --max-retries 3 \
  --region $REGION \
  --project $PROJECT_ID

echo "Cloud Run job updated successfully with increased resources"
echo "CPU: 8 cores"
echo "Memory: 16GB"
echo "Timeout: 3600s (1 hour)"
echo "Max retries: 3"

# Execute a test run with the new configuration if requested
if [[ "$1" == "--run" ]]; then
  echo "Executing the job with the new configuration..."
  gcloud run jobs execute $JOB_NAME \
    --region $REGION \
    --project $PROJECT_ID
  
  echo "Job execution started. Check the logs for progress."
fi

echo "Done!" 