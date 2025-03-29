#!/bin/bash
set -e

# Configuration
PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
WORKFLOW_NAME="reddit-data-workflow"
SENTIMENT_JOB_NAME="reddit-etl-job"
STOCK_JOB_NAME="reddit-stock-etl-job"
SCHEDULER_NAME="reddit-workflow-trigger"
SERVICE_ACCOUNT="workflow-runner@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Deploying components for project: ${PROJECT_ID} in region: ${REGION}"

# Create service account if it doesn't exist
if ! gcloud iam service-accounts describe ${SERVICE_ACCOUNT} &>/dev/null; then
  echo "Creating service account: ${SERVICE_ACCOUNT}"
  gcloud iam service-accounts create workflow-runner --display-name="Workflow Runner"
fi

# Grant necessary roles to service account
echo "Granting roles to service account"
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/workflows.invoker"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/run.invoker"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/cloudfunctions.invoker"

# Build and deploy the sentiment ETL Cloud Run Job
echo "Building and deploying Sentiment ETL Cloud Run Job: ${SENTIMENT_JOB_NAME}"
cd reddit-etl-job
gcloud builds submit --tag gcr.io/${PROJECT_ID}/reddit-etl
gcloud run jobs create ${SENTIMENT_JOB_NAME} \
  --image gcr.io/${PROJECT_ID}/reddit-etl \
  --region ${REGION} \
  --set-env-vars="GOOGLE_CLOUD_PROJECT_ID=${PROJECT_ID}" \
  --set-env-vars="DB_HOST=YOUR_DB_HOST,DB_NAME=YOUR_DB_NAME,DB_USER=YOUR_DB_USER,DB_PASSWORD=YOUR_DB_PASSWORD" \
  --service-account ${SERVICE_ACCOUNT}
cd ..

# Build and deploy the stock ETL Cloud Run Job
echo "Building and deploying Stock ETL Cloud Run Job: ${STOCK_JOB_NAME}"
cd reddit-etl-job
# Copy the directory to create a stock-specific version
cd ..
cp -r reddit-etl-job reddit-stock-etl-job

# Rename Dockerfile entry point
sed -i 's/main.py/stock_main.py/g' reddit-stock-etl-job/Dockerfile
# Move main.py to stock_main.py
mv reddit-stock-etl-job/main.py reddit-stock-etl-job/stock_main.py

# Build and deploy the stock ETL job
cd reddit-stock-etl-job
gcloud builds submit --tag gcr.io/${PROJECT_ID}/reddit-stock-etl
gcloud run jobs create ${STOCK_JOB_NAME} \
  --image gcr.io/${PROJECT_ID}/reddit-stock-etl \
  --region ${REGION} \
  --set-env-vars="GOOGLE_CLOUD_PROJECT_ID=${PROJECT_ID}" \
  --set-env-vars="DB_HOST=YOUR_DB_HOST,DB_NAME=YOUR_DB_NAME,DB_USER=YOUR_DB_USER,DB_PASSWORD=YOUR_DB_PASSWORD" \
  --service-account ${SERVICE_ACCOUNT}
cd ..

# Deploy workflow 
echo "Deploying workflow: ${WORKFLOW_NAME}"
sed -i "s/PROJECT_ID/${PROJECT_ID}/g" scheduler-config.yaml
sed -i "s/LOCATION/${REGION}/g" scheduler-config.yaml

gcloud workflows deploy ${WORKFLOW_NAME} \
  --source=workflow.yaml \
  --region=${REGION} \
  --service-account=${SERVICE_ACCOUNT}

# Create Cloud Scheduler job
echo "Creating Cloud Scheduler job: ${SCHEDULER_NAME}"
gcloud scheduler jobs create http ${SCHEDULER_NAME} \
  --location=${REGION} \
  --schedule="0 */2 * * *" \
  --uri="https://workflowexecutions.googleapis.com/v1/projects/${PROJECT_ID}/locations/${REGION}/workflows/${WORKFLOW_NAME}/executions" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --oauth-service-account-email=${SERVICE_ACCOUNT} \
  --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
  --message-body="{}"

echo "Deployment completed successfully!"
echo "The workflow will run every 2 hours via Cloud Scheduler."
echo "To run the workflow manually, use:"
echo "gcloud workflows run ${WORKFLOW_NAME} --region=${REGION}" 