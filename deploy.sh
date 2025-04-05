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

# Function to check if role binding exists before adding
add_role_if_needed() {
  local sa=$1
  local role=$2
  echo "Checking if role ${role} exists for ${sa}..."
  
  if gcloud projects get-iam-policy ${PROJECT_ID} --format=json | grep -q "\"role\": \"${role}\",.*\"serviceAccount:${sa}\""; then
    echo "Role ${role} already exists for ${sa}. Skipping."
  else
    echo "Adding role ${role} to ${sa}..."
    gcloud projects add-iam-policy-binding ${PROJECT_ID} \
      --member="serviceAccount:${sa}" \
      --role="${role}"
  fi
}

# Grant necessary roles only if they don't already exist
echo "Checking and granting roles to service account if needed"
add_role_if_needed ${SERVICE_ACCOUNT} "roles/workflows.invoker"
add_role_if_needed ${SERVICE_ACCOUNT} "roles/bigquery.jobUser"
add_role_if_needed ${SERVICE_ACCOUNT} "roles/bigquery.dataEditor"
add_role_if_needed ${SERVICE_ACCOUNT} "roles/run.invoker"
add_role_if_needed ${SERVICE_ACCOUNT} "roles/cloudfunctions.invoker"

# Build and deploy the sentiment ETL Cloud Run Job
echo "Building and deploying Sentiment ETL Cloud Run Job: ${SENTIMENT_JOB_NAME}"
cd reddit-etl-job
gcloud builds submit --tag gcr.io/${PROJECT_ID}/reddit-etl

# Function to handle job deployment with proper error handling
deploy_job() {
  local job_name=$1
  local image=$2
  local job_exists=false
  
  # Check if job exists using a more reliable method
  if gcloud run jobs describe ${job_name} --region=${REGION} 2>/dev/null; then
    job_exists=true
  fi
  
  if [ "$job_exists" = true ]; then
    echo "Updating existing job: ${job_name}"
    # Try to update, if it fails, delete and recreate
    if ! gcloud run jobs update ${job_name} \
      --image ${image} \
      --region ${REGION} \
      --set-env-vars="GOOGLE_CLOUD_PROJECT_ID=${PROJECT_ID}" \
      --set-env-vars="DB_HOST=YOUR_DB_HOST,DB_NAME=YOUR_DB_NAME,DB_USER=YOUR_DB_USER,DB_PASSWORD=YOUR_DB_PASSWORD" \
      --service-account ${SERVICE_ACCOUNT}; then
      
      echo "Update failed, deleting and recreating job: ${job_name}"
      gcloud run jobs delete ${job_name} --region=${REGION} --quiet
      
      gcloud run jobs create ${job_name} \
        --image ${image} \
        --region ${REGION} \
        --set-env-vars="GOOGLE_CLOUD_PROJECT_ID=${PROJECT_ID}" \
        --set-env-vars="DB_HOST=YOUR_DB_HOST,DB_NAME=YOUR_DB_NAME,DB_USER=YOUR_DB_USER,DB_PASSWORD=YOUR_DB_PASSWORD" \
        --service-account ${SERVICE_ACCOUNT}
    fi
  else
    echo "Creating new job: ${job_name}"
    gcloud run jobs create ${job_name} \
      --image ${image} \
      --region ${REGION} \
      --set-env-vars="GOOGLE_CLOUD_PROJECT_ID=${PROJECT_ID}" \
      --set-env-vars="DB_HOST=YOUR_DB_HOST,DB_NAME=YOUR_DB_NAME,DB_USER=YOUR_DB_USER,DB_PASSWORD=YOUR_DB_PASSWORD" \
      --service-account ${SERVICE_ACCOUNT}
  fi
}

# Deploy the sentiment ETL job
deploy_job ${SENTIMENT_JOB_NAME} gcr.io/${PROJECT_ID}/reddit-etl
cd ..

# Build and deploy the stock ETL Cloud Run Job
echo "Building and deploying Stock ETL Cloud Run Job: ${STOCK_JOB_NAME}"

# Only copy and set up the directory if it doesn't exist yet
if [ ! -d "reddit-stock-etl-job" ]; then
  echo "Creating reddit-stock-etl-job directory"
  cp -r reddit-etl-job reddit-stock-etl-job
  # Rename Dockerfile entry point
  sed -i 's/main.py/stock_main.py/g' reddit-stock-etl-job/Dockerfile
  # Move main.py to stock_main.py
  cp reddit-stock-etl-job/main.py reddit-stock-etl-job/stock_main.py
else
  echo "Using existing reddit-stock-etl-job directory"
fi

# Build and deploy the stock ETL job
cd reddit-stock-etl-job
gcloud builds submit --tag gcr.io/${PROJECT_ID}/reddit-stock-etl

# Deploy the stock ETL job
deploy_job ${STOCK_JOB_NAME} gcr.io/${PROJECT_ID}/reddit-stock-etl
cd ..

# Deploy workflow 
echo "Deploying workflow: ${WORKFLOW_NAME}"
sed -i "s/PROJECT_ID/${PROJECT_ID}/g" scheduler-config.yaml
sed -i "s/LOCATION/${REGION}/g" scheduler-config.yaml

gcloud workflows deploy ${WORKFLOW_NAME} \
  --source=workflow.yaml \
  --region=${REGION} \
  --service-account=${SERVICE_ACCOUNT}

# Create or update Cloud Scheduler job
echo "Setting up Cloud Scheduler job: ${SCHEDULER_NAME}"
if gcloud scheduler jobs describe ${SCHEDULER_NAME} --location=${REGION} 2>/dev/null; then
  echo "Updating existing scheduler job: ${SCHEDULER_NAME}"
  gcloud scheduler jobs update http ${SCHEDULER_NAME} \
    --location=${REGION} \
    --schedule="0 */2 * * *" \
    --uri="https://workflowexecutions.googleapis.com/v1/projects/${PROJECT_ID}/locations/${REGION}/workflows/${WORKFLOW_NAME}/executions" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --oauth-service-account-email=${SERVICE_ACCOUNT} \
    --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
    --message-body="{}"
else
  echo "Creating new scheduler job: ${SCHEDULER_NAME}"
  gcloud scheduler jobs create http ${SCHEDULER_NAME} \
    --location=${REGION} \
    --schedule="0 */2 * * *" \
    --uri="https://workflowexecutions.googleapis.com/v1/projects/${PROJECT_ID}/locations/${REGION}/workflows/${WORKFLOW_NAME}/executions" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --oauth-service-account-email=${SERVICE_ACCOUNT} \
    --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
    --message-body="{}"
fi

echo "Deployment completed successfully!"
echo "The workflow will run every 2 hours via Cloud Scheduler."
echo "To run the workflow manually, use:"
echo "gcloud workflows run ${WORKFLOW_NAME} --region=${REGION}" 