# Reddit Data Pipeline with Google Cloud Workflows

This project implements a data pipeline that scrapes Reddit data, performs transformations in BigQuery, extracts stock-related mentions, and loads the results into a PostgreSQL database for use by a website backend.

## Architecture

```
Cloud Scheduler
     │ (every 2 hours)
     ▼
Google Cloud Workflows (DAG orchestration)
     ├─▶ Cloud Function (Reddit scraper → BigQuery ingestion)
     │
     ├─▶ BigQuery (Deduplication of raw data)
     │
     ├─▶ BigQuery (SQL transformations)
     │
     ├─▶ Cloud Run Job (Stock ETL extraction → PostgreSQL)
     │     └─▶ Time-based stock aggregations (hourly, daily, weekly)
     │
     └─▶ Cloud Run Job (Sentiment ETL → PostgreSQL)
            └─▶ Final DB → Website Backend
```

## Components

1. **Existing Cloud Function**: Scrapes Reddit data and ingests it into BigQuery
2. **Cloud Workflow**: Orchestrates the entire data pipeline
3. **Data Deduplication**: Removes duplicate Reddit messages from BigQuery
4. **BigQuery Transformations**: Processes the raw data to extract sentiment and aggregate by subreddit
5. **Stock ETL Job**: Extracts stock mentions, performs stock-specific sentiment analysis
6. **Time-based Aggregations**: Provides hourly, daily, and weekly summaries for efficient querying
7. **Sentiment ETL Job**: Extracts general sentiment data from BigQuery and loads into PostgreSQL
8. **Cloud Scheduler**: Triggers the workflow every 2 hours

## Enhanced Features

### Incremental Processing

The pipeline tracks the last successful ETL run and processes only new data since that timestamp:

- **State Management**: Uses Firestore to store the last run timestamp
- **Incremental Data Fetching**: Only processes Reddit data created after the last run
- **Smart Aggregation Updates**: When new data is processed, the aggregation tables are updated incrementally rather than replaced
- **Deduplication**: Avoids processing the same messages multiple times across runs

This significantly improves efficiency when running frequently on large datasets.

### Deduplication

The pipeline includes a dedicated step to remove duplicate Reddit posts and comments from the BigQuery raw messages table, ensuring data quality and preventing inflation of stock mention counts.

### Advanced Signal Detection

The stock analysis extracts multiple types of signals:

- **Trading Signals**: BUY, SELL, HOLD indicators based on context
- **Price Targets**: Extracts specific price targets mentioned for stocks
- **Topic Signals**: Identifies discussions about:
  - Earnings and financial performance
  - News and catalysts
  - Technical analysis
  - Options and derivatives

### Confidence Scoring

Each stock mention is assigned a confidence score based on:

- Sentiment strength (intensity of positive/negative sentiment)
- Reddit post score (upvotes)

This helps identify and prioritize higher quality mentions in the aggregations.

### Time-based Aggregation Tables

For optimal query performance and different time-scale analyses, the pipeline creates multiple aggregation tables:

- **Hourly summaries**: Real-time tracking for the most recent 24 hours
- **Daily summaries**: Day-by-day tracking with detailed signals and contexts
- **Weekly summaries**: Broader trend analysis with day-of-week breakdowns

This design allows the frontend to quickly access pre-aggregated data at the appropriate time scale without expensive on-the-fly calculations.

## PostgreSQL Database Schema

The pipeline creates the following tables in PostgreSQL:

1. **sentiment_analysis**: General sentiment by subreddit
    - `id`: Primary key
    - `subreddit`: Name of the subreddit
    - `message_count`: Number of messages in the subreddit
    - `avg_sentiment`: Average sentiment score
    - `analysis_timestamp`: When the analysis was performed
    - `etl_timestamp`: When the ETL job ran

2. **stock_mentions**: Individual stock mentions in Reddit posts
    - `id`: Primary key
    - `message_id`: ID of the Reddit message
    - `ticker`: Stock ticker symbol
    - `author`: Reddit username
    - `created_at`: When the post was created
    - `subreddit`: Subreddit where it was posted
    - `url`: Link to the post
    - `score`: Reddit score/upvotes
    - `message_type`: POST or COMMENT
    - `sentiment_compound`: Overall sentiment score
    - `sentiment_positive`: Positive sentiment score
    - `sentiment_negative`: Negative sentiment score
    - `sentiment_neutral`: Neutral sentiment score
    - `signals`: Trading signals detected
    - `context`: Extracted text context around the ticker
    - `confidence`: Confidence score for this mention
    - `etl_timestamp`: When the ETL job ran

3. **stock_daily_summary**: Aggregated daily statistics by stock
    - `id`: Primary key
    - `ticker`: Stock ticker symbol
    - `date`: Date of the summary
    - `mention_count`: Number of mentions that day
    - `avg_sentiment`: Average sentiment score
    - `weighted_sentiment`: Sentiment weighted by confidence
    - `buy_signals`: Count of BUY signals
    - `sell_signals`: Count of SELL signals
    - `hold_signals`: Count of HOLD signals
    - `price_targets`: Extracted price targets
    - `news_signals`: Count of NEWS signals
    - `earnings_signals`: Count of EARNINGS signals
    - `technical_signals`: Count of TECHNICAL signals
    - `options_signals`: Count of OPTIONS signals
    - `avg_confidence`: Average confidence score
    - `high_conf_sentiment`: Sentiment of high confidence mentions
    - `top_contexts`: Most relevant contexts with high confidence
    - `subreddits`: Breakdown of mentions by subreddit
    - `etl_timestamp`: When the ETL job ran

4. **stock_hourly_summary**: Aggregated hourly statistics for recent data
    - `id`: Primary key
    - `ticker`: Stock ticker symbol
    - `hour_start`: Start time of the hour
    - `mention_count`: Number of mentions that hour
    - `avg_sentiment`: Average sentiment score
    - `weighted_sentiment`: Sentiment weighted by confidence
    - `buy_signals`, `sell_signals`, `hold_signals`: Signal counts
    - `avg_confidence`: Average confidence score
    - `subreddits`: Breakdown of mentions by subreddit
    - `etl_timestamp`: When the ETL job ran

5. **stock_weekly_summary**: Aggregated weekly statistics for trend analysis
    - `id`: Primary key
    - `ticker`: Stock ticker symbol
    - `week_start`: Start date of the week
    - `mention_count`: Number of mentions that week
    - `avg_sentiment` and `weighted_sentiment`: Sentiment scores
    - Various signal counts and price targets
    - `daily_breakdown`: Mentions by day of week
    - `subreddits`: Breakdown by subreddit
    - `etl_timestamp`: When the ETL job ran

## Prerequisites

- Google Cloud Platform account
- Google Cloud SDK installed
- Existing Cloud Function set up for Reddit scraping
- PostgreSQL database for the final data storage
- Required IAM permissions for deployment
- Firestore database for ETL state tracking

## Environment Variables

Create `.env` files in the ETL job directories with the following variables:

```
# BigQuery
GOOGLE_CLOUD_PROJECT_ID=your-project-id
BIGQUERY_DATASET=reddit_data
BIGQUERY_TABLE=sentiment_by_subreddit

# PostgreSQL
DB_HOST=your-postgres-host
DB_PORT=5432
DB_NAME=your-db-name
DB_USER=your-db-user
DB_PASSWORD=your-db-password
```

## Setup Instructions

1. Clone this repository:
   ```
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Update the deployment script with your specific configuration:
   - Edit `deploy.sh` to update any environment-specific values
   - In particular, update the PostgreSQL connection details in the Cloud Run Job deployment

3. Make the deployment script executable:
   ```
   chmod +x deploy.sh
   ```

4. Run the deployment script:
   ```
   ./deploy.sh
   ```

## Running Manually

To run the workflow manually:

```
gcloud workflows run reddit-data-workflow --region=us-central1
```

## Monitoring

- **Cloud Workflow Executions**: Monitor workflow runs in the Google Cloud Console
- **Cloud Function Logs**: Check the existing Reddit scraper function logs
- **BigQuery Job History**: View BigQuery transformation jobs
- **Cloud Run Job Executions**: Monitor ETL job runs and view logs
- **Firestore ETL State**: Check the `etl_state` collection to view the last run timestamp

## Customization

- **BigQuery Transformations**: Modify the SQL query in `workflow.yaml` to customize the data transformations
- **Stock ETL Job**: Update `reddit-stock-etl-job/stock_main.py` to modify stock detection logic, ticker list, or sentiment analysis
- **ETL Job**: Update `reddit-etl-job/main.py` to change the general ETL logic or target PostgreSQL schema
- **Schedule**: Modify the cron expression in `deploy.sh` to change the frequency of execution 