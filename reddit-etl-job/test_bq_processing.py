#!/usr/bin/env python3
"""
Test script to retrieve and process 100 messages from BigQuery.

This script:
1. Uses a direct query to retrieve 100 messages from BigQuery
2. Processes them using the existing analysis function
3. Writes the results to a temporary BigQuery table
"""
import os
import sys
import logging
import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def check_environment():
    """Check if required environment variables are set."""
    required_vars = [
        'GOOGLE_CLOUD_PROJECT_ID',
        'BIGQUERY_DATASET'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    
    return True

# Synchronous wrapper for async activities
def run_async_activity(activity_func, *args, **kwargs):
    """Run an async activity function synchronously."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(activity_func(*args, **kwargs))
        return result
    finally:
        loop.close()

def write_to_temp_table(processed_data):
    """
    Write processed data to a temporary BigQuery table.
    
    Args:
        processed_data: List of processed stock mentions
        
    Returns:
        Tuple of (number of rows written, table reference)
    """
    if not processed_data:
        logger.warning("No data to write")
        return 0, None
    
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT_ID')
    dataset_id = os.getenv('BIGQUERY_DATASET')
    temp_table_id = f"temp_stock_mentions_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    table_ref = f"{project_id}.{dataset_id}.{temp_table_id}"
    
    logger.info(f"Writing {len(processed_data)} processed entries to temporary table: {table_ref}")
    
    # Create dataframe from the processed data
    import pandas as pd
    
    # Convert stock mentions objects to dictionaries
    data_dicts = [mention.__dict__ for mention in processed_data]
    df = pd.DataFrame(data_dicts)
    
    # Get BigQuery client
    client = bigquery.Client(project=project_id)
    
    # Write to BigQuery
    job_config = bigquery.LoadJobConfig(
        # Write disposition options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        write_disposition="WRITE_TRUNCATE",
    )
    
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete
    
    table = client.get_table(table_ref)
    logger.info(f"Loaded {table.num_rows} rows to {table_ref}")
    
    return table.num_rows, table_ref

def get_test_messages(limit=100):
    """
    Direct method to get test messages from BigQuery, bypassing the extractor.
    
    Args:
        limit: Number of messages to retrieve
    
    Returns:
        DataFrame with messages
    """
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT_ID')
    dataset_id = os.getenv('BIGQUERY_DATASET')
    raw_table_id = 'raw_messages'
    
    logger.info(f"Executing direct query to retrieve {limit} messages")
    
    # Fix the SQL query to use a different timestamp field or remove it entirely
    query = f"""
    SELECT
        message_id,
        content,
        author,
        created_at,
        subreddit,
        title,
        url,
        score,
        message_type
    FROM
        `{project_id}.{dataset_id}.{raw_table_id}`
    WHERE
        content IS NOT NULL
        AND LENGTH(content) > 0
        AND content != '[deleted]'
    ORDER BY
        created_at DESC
    LIMIT {limit}
    """
    
    client = bigquery.Client(project=project_id)
    query_job = client.query(query)
    
    # Convert to dataframe
    df = query_job.to_dataframe()
    
    logger.info(f"Retrieved {len(df)} messages from BigQuery using direct query")
    return df

def delete_temp_table(table_ref):
    """
    Delete a temporary BigQuery table.
    
    Args:
        table_ref: Full reference to the BigQuery table (project.dataset.table)
    """
    logger.info(f"Deleting temporary table: {table_ref}")
    client = bigquery.Client(project=os.getenv('GOOGLE_CLOUD_PROJECT_ID'))
    
    try:
        client.delete_table(table_ref)
        logger.info(f"Successfully deleted temporary table: {table_ref}")
    except Exception as e:
        logger.error(f"Failed to delete temporary table {table_ref}: {str(e)}")

def main():
    """Main function to execute the test."""
    logger.info("Starting BigQuery test processing job with 100 message limit")
    
    # Check environment variables
    if not check_environment():
        logger.error("Environment check failed, exiting")
        sys.exit(1)
    
    try:
        # Import the existing functions from the codebase
        from src.activities.analysis_activities import analyze_stock_mentions_activity
        
        # Use direct method to get test messages instead of using the extractor
        df = get_test_messages(100)
        
        if df.empty:
            logger.warning("No data found in BigQuery")
            sys.exit(0)
        
        # Convert DataFrame to list of dictionaries for the analysis function
        reddit_data = df.to_dict('records')
        logger.info(f"Retrieved {len(reddit_data)} Reddit posts/comments from BigQuery")
        
        # Process the data using the existing analysis function
        stock_mentions = run_async_activity(analyze_stock_mentions_activity, reddit_data)
        logger.info(f"Found {len(stock_mentions)} stock mentions")
        
        # Write the processed data to a temporary table
        rows_written, table_ref = write_to_temp_table(stock_mentions)
        
        logger.info(f"Test completed successfully. Processed and wrote {rows_written} rows to temporary table.")
        
        # Delete the temporary table after successful processing
        if table_ref:
            delete_temp_table(table_ref)
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 