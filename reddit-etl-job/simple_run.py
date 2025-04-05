#!/usr/bin/env python3
"""
Simple standalone runner for Reddit ETL Job

This script bypasses Temporal and directly runs the ETL process.
"""
import logging
import os
import sys
import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def check_environment():
    """Check if all required environment variables are set."""
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

# Synchronous wrappers for async activities
def run_async_activity(activity_func, *args, **kwargs):
    """Run an async activity function synchronously."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(activity_func(*args, **kwargs))
        return result
    finally:
        loop.close()

def main():
    """Main entry point for the simple ETL runner."""
    logger.info("Starting Reddit ETL job for stock analysis (Simple Runner)")
    
    # Check environment variables
    if not check_environment():
        logger.error("Environment check failed, exiting")
        sys.exit(1)
    
    try:
        # Import activities directly
        from src.activities.extraction_activities import extract_reddit_data_activity
        from src.activities.analysis_activities import analyze_stock_mentions_activity
        from src.activities.aggregation_activities import (
            aggregate_daily_summaries_activity,
            aggregate_hourly_summaries_activity,
            aggregate_weekly_summaries_activity
        )
        from src.activities.persistence_activities import (
            save_stock_mentions_activity,
            save_daily_summaries_activity,
            save_hourly_summaries_activity,
            save_weekly_summaries_activity
        )
        from src.activities.state_activities import get_last_run_activity, update_run_timestamp_activity

        # Set up BigQuery
        from src.utils.bigquery_utils import BigQueryManager
        bq_manager = BigQueryManager()
        bq_manager.setup_tables()
        
        # Define start and end time (last 24 hours)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=24)
        
        # Run the ETL process directly
        logger.info(f"Extracting data from {start_time} to {end_time}")
        
        # Get the last run timestamp
        last_run_time = run_async_activity(get_last_run_activity)
        
        # Extract data with proper time filtering
        reddit_data = run_async_activity(extract_reddit_data_activity, last_run_time)
        logger.info(f"Extracted {len(reddit_data)} Reddit posts/comments")
        
        # Analyze data
        stock_mentions = run_async_activity(analyze_stock_mentions_activity, reddit_data)
        logger.info(f"Found {len(stock_mentions)} stock mentions")
        
        # Save mentions
        save_result = run_async_activity(save_stock_mentions_activity, stock_mentions)
        logger.info(f"Saved {save_result} stock mentions to BigQuery")
        
        # Aggregate data
        daily_summaries = run_async_activity(
            aggregate_daily_summaries_activity, 
            stock_mentions
        )
        
        hourly_summaries = run_async_activity(
            aggregate_hourly_summaries_activity,
            stock_mentions
        )
        
        weekly_summaries = run_async_activity(
            aggregate_weekly_summaries_activity, 
            stock_mentions
        )
        
        # Save aggregated data
        daily_result = run_async_activity(save_daily_summaries_activity, daily_summaries)
        hourly_result = run_async_activity(save_hourly_summaries_activity, hourly_summaries)
        weekly_result = run_async_activity(save_weekly_summaries_activity, weekly_summaries)
        
        # Update the timestamp for the next run
        run_async_activity(update_run_timestamp_activity, end_time)
        logger.info(f"Updated last run timestamp to: {end_time}")
        
        logger.info(f"Saved {daily_result} daily summaries, {hourly_result} hourly summaries, and {weekly_result} weekly summaries")
        logger.info("ETL job completed successfully")
        
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 