# Reddit ETL Job for Stock Analysis

A Cloud Run job that extracts stock-related information from Reddit posts, performs sentiment analysis, and aggregates the data into BigQuery tables.

## Overview

This ETL (Extract, Transform, Load) job processes Reddit posts to identify stock mentions, analyzes the sentiment of those mentions, and aggregates the data into summary tables. The workflow is orchestrated using Temporal.io.

## Data Flow

1. Extract: Pull Reddit data from a BigQuery table containing Reddit posts
2. Transform: Analyze posts to identify stock mentions and perform sentiment analysis
3. Load: Save the processed data and aggregations to BigQuery tables

## BigQuery Tables

The ETL job writes data to the following BigQuery tables:

- `stock_mentions`: Individual stock mentions extracted from Reddit posts
- `stock_daily_summary`: Daily aggregated statistics for each stock
- `stock_hourly_summary`: Hourly aggregated statistics for each stock
- `stock_weekly_summary`: Weekly aggregated statistics for each stock

## Environment Variables

Create a `.env` file based on the `sample.env` template. Required environment variables include:

```
GOOGLE_CLOUD_PROJECT_ID=your-project-id
GCP_REGION=us-central1
BIGQUERY_DATASET=reddit_data
TEMPORAL_HOST=temporal.example.com:7233
TEMPORAL_TASK_QUEUE=reddit-etl-task-queue
```

## Running Locally

1. Install dependencies: `pip install -r requirements.txt`
2. Set up environment variables in a `.env` file
3. Run the main script: `python main.py`

## Deployment

This ETL job is designed to run as a Cloud Run job. Use the `deploy.sh` script to deploy to Google Cloud:

```bash
./deploy.sh
```

## Architecture

The application uses:
- Temporal.io for workflow orchestration
- BigQuery for data storage and retrieval
- Google Cloud Run for serverless execution
- Spacy and other NLP libraries for sentiment analysis

## File Structure

- `main.py`: Entry point for the ETL job
- `src/workflows/`: Temporal workflow definitions
- `src/activities/`: Activity implementations for each ETL step
- `src/models/`: Data models for stock information
- `src/utils/`: Utility functions, including BigQuery operations 