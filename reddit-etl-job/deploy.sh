#!/bin/bash
set -e

# Load environment variables
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

# Check if required environment variables are set
REQUIRED_VARS=(
  "GOOGLE_CLOUD_PROJECT_ID"
  "TEMPORAL_NAMESPACE"
  "GCP_REGION"
  "GCR_REPOSITORY"
  "CLOUD_RUN_SERVICE_NAME"
  "BIGQUERY_DATASET"
  "TEMPORAL_HOST"
  "TEMPORAL_TASK_QUEUE"
)

for var in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!var}" ]; then
    echo "Error: Required environment variable $var is not set."
    exit 1
  fi
done

echo "=== Building and deploying Reddit ETL job ==="

# Set default values if not provided
TEMPORAL_NAMESPACE=${TEMPORAL_NAMESPACE:-"default"}
GCP_REGION=${GCP_REGION:-"us-central1"}
GCR_REPOSITORY=${GCR_REPOSITORY:-"reddit-etl"}
CLOUD_RUN_SERVICE_NAME=${CLOUD_RUN_SERVICE_NAME:-"reddit-etl-job"}

# Generate a timestamp for the image tag
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
IMAGE_TAG="gcr.io/${GOOGLE_CLOUD_PROJECT_ID}/${GCR_REPOSITORY}:${TIMESTAMP}"
LATEST_TAG="gcr.io/${GOOGLE_CLOUD_PROJECT_ID}/${GCR_REPOSITORY}:latest"

# Build the Docker image with BuildKit for better caching
echo "Building Docker image..."
DOCKER_BUILDKIT=1 docker build --cache-from=${LATEST_TAG} -t ${IMAGE_TAG} -t ${LATEST_TAG} .

# Push the image to Google Container Registry
echo "Pushing image to Google Container Registry..."
docker push ${IMAGE_TAG}
docker push ${LATEST_TAG}

# Create service account for the ETL job if it doesn't exist
SERVICE_ACCOUNT_NAME="reddit-etl-job"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${GOOGLE_CLOUD_PROJECT_ID}.iam.gserviceaccount.com"

echo "Checking if service account exists..."
if ! gcloud iam service-accounts describe $SERVICE_ACCOUNT_EMAIL &>/dev/null; then
  echo "Creating service account for ETL job..."
  gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
    --display-name="Service Account for Reddit ETL Job"
  
  # Wait for service account to be fully created
  echo "Waiting for service account to be fully created..."
  sleep 10
  
  # Verify service account exists
  if ! gcloud iam service-accounts describe $SERVICE_ACCOUNT_EMAIL &>/dev/null; then
    echo "Error: Failed to create service account"
    exit 1
  fi
fi

# Give the service account necessary permissions
echo "Setting up IAM permissions..."

# Permission to access BigQuery
echo "Adding BigQuery data editor role..."
gcloud projects add-iam-policy-binding ${GOOGLE_CLOUD_PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
  --role="roles/bigquery.dataEditor"

# Permission to create BigQuery datasets
echo "Adding BigQuery data owner role..."
gcloud projects add-iam-policy-binding ${GOOGLE_CLOUD_PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
  --role="roles/bigquery.dataOwner"

# Permission to run BigQuery jobs (important for job creation)
echo "Adding BigQuery job user role..."
gcloud projects add-iam-policy-binding ${GOOGLE_CLOUD_PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
  --role="roles/bigquery.jobUser"

# Permission to access Firestore/Datastore (for storing run timestamps)
echo "Adding Firestore/Datastore user role..."
gcloud projects add-iam-policy-binding ${GOOGLE_CLOUD_PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
  --role="roles/datastore.user"

# Deploy the worker as a Cloud Run job (not a service)
echo "Deploying as Cloud Run job..."

# Check if job already exists
if gcloud run jobs describe $CLOUD_RUN_SERVICE_NAME --region $GCP_REGION &>/dev/null; then
  echo "Updating existing Cloud Run job..."
  gcloud run jobs update $CLOUD_RUN_SERVICE_NAME \
    --image $IMAGE_TAG \
    --region $GCP_REGION \
    --tasks 1 \
    --memory 32Gi \
    --cpu 8 \
    --max-retries 3 \
    --task-timeout 28800 \
    --service-account $SERVICE_ACCOUNT_EMAIL \
    --set-env-vars="TEMPORAL_NAMESPACE=${TEMPORAL_NAMESPACE}" \
    --set-env-vars="TEMPORAL_HOST=${TEMPORAL_HOST}" \
    --set-env-vars="TEMPORAL_TASK_QUEUE=${TEMPORAL_TASK_QUEUE}" \
    --set-env-vars="GOOGLE_CLOUD_PROJECT_ID=${GOOGLE_CLOUD_PROJECT_ID}" \
    --set-env-vars="BIGQUERY_DATASET=${BIGQUERY_DATASET}"
else
  echo "Creating new Cloud Run job..."
  gcloud run jobs create $CLOUD_RUN_SERVICE_NAME \
    --image $IMAGE_TAG \
    --region $GCP_REGION \
    --tasks 1 \
    --memory 32Gi \
    --cpu 8 \
    --max-retries 3 \
    --task-timeout 28800 \
    --service-account $SERVICE_ACCOUNT_EMAIL \
    --set-env-vars="TEMPORAL_NAMESPACE=${TEMPORAL_NAMESPACE}" \
    --set-env-vars="TEMPORAL_HOST=${TEMPORAL_HOST}" \
    --set-env-vars="TEMPORAL_TASK_QUEUE=${TEMPORAL_TASK_QUEUE}" \
    --set-env-vars="GOOGLE_CLOUD_PROJECT_ID=${GOOGLE_CLOUD_PROJECT_ID}" \
    --set-env-vars="BIGQUERY_DATASET=${BIGQUERY_DATASET}"
fi

echo "=== Deployment completed successfully ==="
echo "Job name: $CLOUD_RUN_SERVICE_NAME"
echo "Image: $IMAGE_TAG"
echo "BigQuery dataset: $BIGQUERY_DATASET"
echo ""
echo "To run the job manually, use:"
echo "gcloud run jobs execute $CLOUD_RUN_SERVICE_NAME --region $GCP_REGION" 