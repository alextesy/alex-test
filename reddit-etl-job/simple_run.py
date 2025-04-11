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
from typing import List, Dict, Any
from src.models.stock_data import StockMention

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

def setup_bigquery():
    """Set up BigQuery tables."""
    from src.utils.bigquery_utils import BigQueryManager
    bq_manager = BigQueryManager()
    bq_manager.setup_tables()
    return bq_manager

def extract_reddit_data(current_time) -> List[Dict[str, Any]]:
    """Extract data from Reddit."""
    from src.activities.extraction_activities import extract_reddit_data_activity
    from src.activities.state_activities import (
        get_step_last_run_activity,
        update_step_timestamp_activity,
        STEP_EXTRACTION
    )
    
    extraction_last_run = run_async_activity(get_step_last_run_activity, STEP_EXTRACTION)
    logger.info(f"Extracting data since: {extraction_last_run}")
    reddit_data = run_async_activity(extract_reddit_data_activity, extraction_last_run)
    logger.info(f"Extracted {len(reddit_data)} Reddit posts/comments")
    # Update extraction timestamp
    run_async_activity(update_step_timestamp_activity, STEP_EXTRACTION, current_time)
    
    return reddit_data

def analyze_stock_mentions(reddit_data: List[Dict[str, Any]], current_time: datetime):
    """Analyze data for stock mentions."""
    from src.activities.analysis_activities import analyze_stock_mentions_activity
    from src.activities.state_activities import (
        update_step_timestamp_activity,
        STEP_ANALYSIS
    )
    
    run_async_activity(analyze_stock_mentions_activity, reddit_data)
    # Update analysis timestamp
    run_async_activity(update_step_timestamp_activity, STEP_ANALYSIS, current_time)
    

def aggregate_summaries(stock_mentions, current_time):
    """Aggregate summaries at different time intervals."""
    from src.activities.aggregation_activities import (
        aggregate_daily_summaries_activity,
        aggregate_hourly_summaries_activity,
        aggregate_weekly_summaries_activity
    )
    from src.activities.state_activities import (
        update_step_timestamp_activity,
        STEP_DAILY_AGGREGATION,
        STEP_HOURLY_AGGREGATION,
        STEP_WEEKLY_AGGREGATION
    )
    
    # Aggregate daily summaries
    daily_summaries = run_async_activity(
        aggregate_daily_summaries_activity, 
        stock_mentions
    )
    # Update daily aggregation timestamp
    run_async_activity(update_step_timestamp_activity, STEP_DAILY_AGGREGATION, current_time)
    
    # Aggregate hourly summaries
    hourly_summaries = run_async_activity(
        aggregate_hourly_summaries_activity,
        stock_mentions
    )
    # Update hourly aggregation timestamp
    run_async_activity(update_step_timestamp_activity, STEP_HOURLY_AGGREGATION, current_time)
    
    # Aggregate weekly summaries
    weekly_summaries = run_async_activity(
        aggregate_weekly_summaries_activity, 
        stock_mentions
    )
    # Update weekly aggregation timestamp
    run_async_activity(update_step_timestamp_activity, STEP_WEEKLY_AGGREGATION, current_time)
    
    return daily_summaries, hourly_summaries, weekly_summaries

def save_aggregated_data(daily_summaries, hourly_summaries, weekly_summaries, current_time):
    """Save aggregated data to BigQuery."""
    from src.activities.persistence_activities import (
        save_daily_summaries_activity,
        save_hourly_summaries_activity,
        save_weekly_summaries_activity
    )
    from src.activities.state_activities import (
        update_step_timestamp_activity,
        STEP_DAILY_PERSISTENCE,
        STEP_HOURLY_PERSISTENCE,
        STEP_WEEKLY_PERSISTENCE
    )
    
    # Save daily summaries
    daily_result = run_async_activity(save_daily_summaries_activity, daily_summaries)
    # Update daily persistence timestamp
    run_async_activity(update_step_timestamp_activity, STEP_DAILY_PERSISTENCE, current_time)
    
    # Save hourly summaries
    hourly_result = run_async_activity(save_hourly_summaries_activity, hourly_summaries)
    # Update hourly persistence timestamp
    run_async_activity(update_step_timestamp_activity, STEP_HOURLY_PERSISTENCE, current_time)
    
    # Save weekly summaries
    weekly_result = run_async_activity(save_weekly_summaries_activity, weekly_summaries)
    # Update weekly persistence timestamp
    run_async_activity(update_step_timestamp_activity, STEP_WEEKLY_PERSISTENCE, current_time)
    
    logger.info(f"Saved {daily_result} daily summaries, {hourly_result} hourly summaries, and {weekly_result} weekly summaries")
    
    return daily_result, hourly_result, weekly_result

def main():
    """Main entry point for the simple ETL runner."""
    logger.info("Starting Reddit ETL job for stock analysis (Simple Runner)")
    
    # Check environment variables
    if not check_environment():
        logger.error("Environment check failed, exiting")
        sys.exit(1)
    
    try:
        # Set up BigQuery
        setup_bigquery()
        
        # Current time used for all timestamp updates
        current_time = datetime.utcnow()
        logger.info(f"Current time: {current_time}")
        
        # 1. Extract data from Reddit
        reddit_data = extract_reddit_data(current_time)
        
        # 2. Analyze data for stock mentions
        analyze_stock_mentions(reddit_data, current_time)
        
        # # 3. Aggregate summaries
        # daily_summaries, hourly_summaries, weekly_summaries = aggregate_summaries(stock_mentions, current_time)
        
        # # 4. Save aggregated data
        # save_aggregated_data(daily_summaries, hourly_summaries, weekly_summaries, current_time)
        
        logger.info("ETL job completed successfully")
        
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 