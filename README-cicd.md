# CI/CD Setup for Reddit ETL Job

## Setting up GitHub Actions with GCP

1. Set up Workload Identity Federation for GitHub Actions:

```bash
# Create a Workload Identity Pool
gcloud iam workload-identity-pools create "github-actions-pool" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --display-name="GitHub Actions Pool"

# Create a Workload Identity Provider in that pool
gcloud iam workload-identity-pools providers create-oidc "github-provider" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --workload-identity-pool="github-actions-pool" \
  --display-name="GitHub provider" \
  --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository" \
  --issuer-uri="https://token.actions.githubusercontent.com"

# Get the Workload Identity Provider resource name
export WIF_PROVIDER=$(gcloud iam workload-identity-pools providers describe github-provider \
  --project="${PROJECT_ID}" \
  --location="global" \
  --workload-identity-pool="github-actions-pool" \
  --format="value(name)")

# Create a dedicated service account for GitHub Actions
gcloud iam service-accounts create github-actions-sa \
  --project="${PROJECT_ID}" \
  --display-name="GitHub Actions service account"

# Grant the service account permissions (adjust as needed)
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:github-actions-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/run.admin"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:github-actions-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.admin"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:github-actions-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"

# Allow the GitHub repo to impersonate the service account
gcloud iam service-accounts add-iam-policy-binding "github-actions-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --project="${PROJECT_ID}" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/${WIF_PROVIDER}/attribute.repository/YOUR_GITHUB_USERNAME/YOUR_REPO_NAME"
```

2. Add the following secrets to your GitHub repository:
   - `WIF_PROVIDER`: The value from the WIF_PROVIDER variable above
   - `GCP_SA_EMAIL`: `github-actions-sa@${PROJECT_ID}.iam.gserviceaccount.com`
   - And other environment variables from your deploy.sh script

## How It Works

This setup allows GitHub Actions to authenticate with GCP without storing service account keys in GitHub secrets. 

1. When a workflow runs, GitHub provides an OpenID Connect token
2. The workflow exchanges this token for a GCP access token using Workload Identity Federation
3. This GCP token is used to authenticate all gcloud and GCP API calls

## Triggering Builds

The workflow will automatically trigger when you push changes to the `main` branch in the `reddit-etl-job` directory. 