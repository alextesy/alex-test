import os
import logging
import json
from google.cloud import bigquery
from google.cloud import logging as cloud_logging
from firebase_admin import initialize_app, credentials, _apps
from firebase_functions import https_fn

# Initialize Firebase Admin
if not _apps:
    cred = credentials.ApplicationDefault()  # Uses GCP's default credentials
    app = initialize_app(cred)

# Get project ID from environment
PROJECT_ID = os.getenv('PROJECT_ID')
if not PROJECT_ID:
    raise ValueError("PROJECT_ID environment variable is not set")

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Setup Google Cloud Logging only in production
if not os.getenv('FUNCTIONS_EMULATOR'):  # Skip in local development
    try:
        client = cloud_logging.Client(project=PROJECT_ID)
        client.setup_logging()
        logger.info("Google Cloud Logging initialized")
    except Exception as e:
        logger.warning(f"Failed to initialize Google Cloud Logging: {e}. Using default logging.")
        # Setup basic logging for local development
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)

@https_fn.on_request(memory=https_fn.options.MemoryOption.GB_1, timeout_sec=540)
def run_bigquery_analysis(req: https_fn.Request) -> https_fn.Response:
    """Cloud Function to run BigQuery analysis on Reddit data.
    
    Args:
        req: The request object
        
    Returns:
        The response object with analysis results
    """
    logger.info("Starting BigQuery analysis")
    
    try:
        # Parse request to get dataset ID
        request_json = req.get_json() if req.is_json else {}
        dataset_id = request_json.get('dataset_id', 'reddit_data')
        
        # Create BigQuery client
        bq_client = bigquery.Client(project=PROJECT_ID)
        
        # Run analysis queries
        analysis_results = run_analysis_queries(bq_client, dataset_id)
        
        # Return success response
        return https_fn.Response(
            json.dumps({"status": "success", "results": analysis_results}),
            status=200,
            headers={"Content-Type": "application/json"}
        )
    except Exception as e:
        error_msg = f"Error running BigQuery analysis: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return https_fn.Response(
            json.dumps({"status": "error", "message": error_msg}),
            status=500,
            headers={"Content-Type": "application/json"}
        )

def run_analysis_queries(bq_client: bigquery.Client, dataset_id: str) -> dict:
    """Run BigQuery analysis queries on Reddit data.
    
    Args:
        bq_client: BigQuery client
        dataset_id: BigQuery dataset ID
        
    Returns:
        dict: Analysis results
    """
    logger.info(f"Running analysis queries on dataset {dataset_id}")
    
    results = {}
    
    try:
        # Get total count of messages by type
        message_count_query = f"""
        SELECT
            message_type,
            COUNT(*) as count
        FROM
            `{PROJECT_ID}.{dataset_id}.raw_messages`
        GROUP BY
            message_type
        ORDER BY
            count DESC
        """
        
        logger.info("Running message count query")
        query_job = bq_client.query(message_count_query)
        message_count_results = list(query_job.result())
        
        # Convert to dict for easy JSON serialization
        message_counts = {row['message_type']: row['count'] for row in message_count_results}
        results['message_counts'] = message_counts
        
        # Get top subreddits by post count
        top_subreddits_query = f"""
        SELECT
            subreddit,
            COUNT(*) as count
        FROM
            `{PROJECT_ID}.{dataset_id}.raw_messages`
        WHERE
            subreddit IS NOT NULL
        GROUP BY
            subreddit
        ORDER BY
            count DESC
        LIMIT 10
        """
        
        logger.info("Running top subreddits query")
        query_job = bq_client.query(top_subreddits_query)
        top_subreddits_results = list(query_job.result())
        
        # Convert to dict for easy JSON serialization
        top_subreddits = [{'subreddit': row['subreddit'], 'count': row['count']} 
                           for row in top_subreddits_results]
        results['top_subreddits'] = top_subreddits
        
        # Get message count by day for the last 30 days
        daily_count_query = f"""
        SELECT
            CAST(created_at AS DATE) as date,
            COUNT(*) as count
        FROM
            `{PROJECT_ID}.{dataset_id}.raw_messages`
        WHERE
            created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        GROUP BY
            date
        ORDER BY
            date
        """
        
        logger.info("Running daily count query")
        query_job = bq_client.query(daily_count_query)
        daily_count_results = list(query_job.result())
        
        # Convert to dict for easy JSON serialization
        daily_counts = [{'date': row['date'].isoformat(), 'count': row['count']} 
                         for row in daily_count_results]
        results['daily_counts'] = daily_counts
        
        # Save analysis results to a summary table
        summary_table_id = f"{PROJECT_ID}.{dataset_id}.analysis_summary"
        
        # Create the table if it doesn't exist
        schema = [
            bigquery.SchemaField("analysis_date", "TIMESTAMP"),
            bigquery.SchemaField("total_messages", "INTEGER"),
            bigquery.SchemaField("total_posts", "INTEGER"),
            bigquery.SchemaField("total_comments", "INTEGER"),
            bigquery.SchemaField("top_subreddit", "STRING"),
            bigquery.SchemaField("top_subreddit_count", "INTEGER")
        ]
        
        table = bigquery.Table(summary_table_id, schema=schema)
        bq_client.create_table(table, exists_ok=True)
        
        # Insert summary row
        total_messages = sum(message_counts.values())
        total_posts = message_counts.get('REDDIT_POST', 0)
        total_comments = message_counts.get('REDDIT_COMMENT', 0)
        top_subreddit = top_subreddits[0]['subreddit'] if top_subreddits else None
        top_subreddit_count = top_subreddits[0]['count'] if top_subreddits else 0
        
        summary_row = {
            'analysis_date': bigquery.ScalarQueryParameter(
                'analysis_date', 'TIMESTAMP', bigquery.CURRENT_TIMESTAMP
            ).value,
            'total_messages': total_messages,
            'total_posts': total_posts,
            'total_comments': total_comments,
            'top_subreddit': top_subreddit,
            'top_subreddit_count': top_subreddit_count
        }
        
        logger.info("Inserting summary row into analysis_summary table")
        errors = bq_client.insert_rows_json(summary_table_id, [summary_row])
        if errors:
            logger.warning(f"Errors inserting summary row: {errors}")
        
        # Add summary to results
        results['summary'] = {
            'total_messages': total_messages,
            'total_posts': total_posts,
            'total_comments': total_comments,
            'top_subreddit': top_subreddit,
            'top_subreddit_count': top_subreddit_count
        }
        
        logger.info("Analysis queries completed successfully")
        return results
    except Exception as e:
        logger.error(f"Error in run_analysis_queries: {str(e)}", exc_info=True)
        raise 