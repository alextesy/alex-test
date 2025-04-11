#!/usr/bin/env python3
"""
Test runner for Reddit ETL Job

This script follows the same flow as simple_run.py but operates
on a small subsample of data from BigQuery for testing and debugging.
"""
import logging
import os
import sys
import asyncio
import argparse
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Helper function to run async activities
def run_async_activity(activity_func, *args, **kwargs):
    """Run an async activity function synchronously."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(activity_func(*args, **kwargs))
        return result
    finally:
        loop.close()

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

def setup_environment(test_mode=True):
    """Set up the environment for testing."""
    # Set environment variables for testing if they're not set
    if 'GOOGLE_CLOUD_PROJECT_ID' not in os.environ:
        os.environ['GOOGLE_CLOUD_PROJECT_ID'] = 'alex-stocks'
    
    if 'BIGQUERY_DATASET' not in os.environ:
        os.environ['BIGQUERY_DATASET'] = 'reddit_stock_test'


    

    # Create a test state collection to avoid affecting production timestamps
    os.environ['FIRESTORE_STATE_COLLECTION'] = 'pipeline_state_test'
    logger.info("Using test state collection: pipeline_state_test")


def setup_bigquery():
    """Set up BigQuery tables."""
    from src.utils.bigquery_utils import BigQueryManager
    bq_manager = BigQueryManager()
    bq_manager.setup_tables()
    return bq_manager

def extract_reddit_data(current_time, hours_back=24, data_limit=100):
    """
    Extract a limited amount of Reddit data from BigQuery.
    
    Args:
        current_time: Current timestamp
        hours_back: Hours to look back for data (if no last run timestamp exists)
        data_limit: Maximum number of records to retrieve
        
    Returns:
        List of Reddit posts/comments
    """
    from src.activities.extraction_activities import extract_reddit_data_activity
    from src.activities.state_activities import (
        get_step_last_run_activity,
        update_step_timestamp_activity,
        STEP_EXTRACTION
    )
    from src.extractors.bigquery_extractor import BigQueryExtractor
    
    # Override the get_reddit_data method to limit the data
    original_get_reddit_data = BigQueryExtractor.get_reddit_data
    
    def limited_get_reddit_data(self, last_run_time=None, limit=None):
        """Override to limit data for testing"""
        if limit is None:
            limit = data_limit
        logger.info(f"Using modified extractor with limit: {limit}")
        return original_get_reddit_data(self, last_run_time, limit)
    
    # Apply the monkey patch for testing
    BigQueryExtractor.get_reddit_data = limited_get_reddit_data
    
    try:
        # Get timestamp of last extraction
        if hours_back:
            # For testing, we can override the last run time to get a specific time range
            extraction_last_run = current_time - timedelta(hours=hours_back)
            logger.info(f"Using override extraction time: {extraction_last_run} (looking back {hours_back} hours)")
        else:
            # Use the stored timestamp for production-like behavior
            extraction_last_run = run_async_activity(get_step_last_run_activity, STEP_EXTRACTION)
            logger.info(f"Using stored extraction time: {extraction_last_run}")
        
        # Extract data from BigQuery
        reddit_data = run_async_activity(extract_reddit_data_activity, extraction_last_run)
        logger.info(f"Extracted {len(reddit_data)} Reddit posts/comments from BigQuery")
        
        # Update extraction timestamp
        run_async_activity(update_step_timestamp_activity, STEP_EXTRACTION, current_time)
        
        return reddit_data
    
    finally:
        # Restore the original method
        BigQueryExtractor.get_reddit_data = original_get_reddit_data

def analyze_stock_mentions(reddit_data, current_time):
    """
    Analyze data for stock mentions.
    
    Args:
        reddit_data: List of Reddit posts/comments
        current_time: Current timestamp
        
    Returns:
        List of StockMention objects
    """
    from src.activities.analysis_activities import analyze_stock_mentions_activity
    from src.activities.state_activities import (
        update_step_timestamp_activity,
        STEP_ANALYSIS
    )
    
    logger.info(f"Analyzing {len(reddit_data)} Reddit posts/comments for stock mentions")
    
    # Analyze data
    stock_mentions = run_async_activity(analyze_stock_mentions_activity, reddit_data)
    logger.info(f"Found {len(stock_mentions)} stock mentions")
    
    # Update analysis timestamp
    run_async_activity(update_step_timestamp_activity, STEP_ANALYSIS, current_time)
    
    return stock_mentions

def save_stock_mentions(stock_mentions, current_time):
    """
    Save stock mentions to BigQuery.
    
    Args:
        stock_mentions: List of StockMention objects
        current_time: Current timestamp
        
    Returns:
        Number of stock mentions saved
    """
    from src.activities.persistence_activities import save_stock_mentions_activity
    from src.activities.state_activities import (
        update_step_timestamp_activity,
        STEP_STOCK_PERSISTENCE
    )
    
    logger.info(f"Saving {len(stock_mentions)} stock mentions to BigQuery")
    
    # Save stock mentions
    save_result = run_async_activity(save_stock_mentions_activity, stock_mentions)
    logger.info(f"Saved {save_result} stock mentions to BigQuery")
    
    # Update persistence timestamp
    run_async_activity(update_step_timestamp_activity, STEP_STOCK_PERSISTENCE, current_time)
    
    return save_result

def aggregate_summaries(stock_mentions, current_time):
    """
    Aggregate summaries at different time intervals.
    
    Args:
        stock_mentions: List of StockMention objects
        current_time: Current timestamp
        
    Returns:
        Tuple of (daily_summaries, hourly_summaries, weekly_summaries)
    """
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
    
    logger.info(f"Aggregating {len(stock_mentions)} stock mentions into summaries")
    
    # Aggregate daily summaries
    daily_summaries = run_async_activity(
        aggregate_daily_summaries_activity, 
        stock_mentions
    )
    # Update daily aggregation timestamp
    run_async_activity(update_step_timestamp_activity, STEP_DAILY_AGGREGATION, current_time)
    logger.info(f"Generated {len(daily_summaries)} daily summaries")
    
    # Aggregate hourly summaries
    hourly_summaries = run_async_activity(
        aggregate_hourly_summaries_activity,
        stock_mentions
    )
    # Update hourly aggregation timestamp
    run_async_activity(update_step_timestamp_activity, STEP_HOURLY_AGGREGATION, current_time)
    logger.info(f"Generated {len(hourly_summaries)} hourly summaries")
    
    # Aggregate weekly summaries
    weekly_summaries = run_async_activity(
        aggregate_weekly_summaries_activity, 
        stock_mentions
    )
    # Update weekly aggregation timestamp
    run_async_activity(update_step_timestamp_activity, STEP_WEEKLY_AGGREGATION, current_time)
    logger.info(f"Generated {len(weekly_summaries)} weekly summaries")
    
    return daily_summaries, hourly_summaries, weekly_summaries

def save_aggregated_data(daily_summaries, hourly_summaries, weekly_summaries, current_time):
    """
    Save aggregated data to BigQuery.
    
    Args:
        daily_summaries: List of DailySummary objects
        hourly_summaries: List of HourlySummary objects
        weekly_summaries: List of WeeklySummary objects
        current_time: Current timestamp
        
    Returns:
        Tuple of (daily_result, hourly_result, weekly_result)
    """
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
    
    logger.info("Saving aggregated data to BigQuery")
    
    # Save daily summaries
    daily_result = run_async_activity(save_daily_summaries_activity, daily_summaries)
    # Update daily persistence timestamp
    run_async_activity(update_step_timestamp_activity, STEP_DAILY_PERSISTENCE, current_time)
    logger.info(f"Saved {daily_result} daily summaries")
    
    # Save hourly summaries
    hourly_result = run_async_activity(save_hourly_summaries_activity, hourly_summaries)
    # Update hourly persistence timestamp
    run_async_activity(update_step_timestamp_activity, STEP_HOURLY_PERSISTENCE, current_time)
    logger.info(f"Saved {hourly_result} hourly summaries")
    
    # Save weekly summaries
    weekly_result = run_async_activity(save_weekly_summaries_activity, weekly_summaries)
    # Update weekly persistence timestamp
    run_async_activity(update_step_timestamp_activity, STEP_WEEKLY_PERSISTENCE, current_time)
    logger.info(f"Saved {weekly_result} weekly summaries")
    
    logger.info(f"Successfully saved {daily_result + hourly_result + weekly_result} summaries to BigQuery")
    
    return daily_result, hourly_result, weekly_result

def main():
    """Main entry point for the test runner."""
    parser = argparse.ArgumentParser(description='Run a test version of the Reddit ETL pipeline')
    parser.add_argument('--data-limit', type=int, default=100,
                        help='Maximum number of records to retrieve from BigQuery')
    parser.add_argument('--hours-back', type=int, default=24,
                        help='Hours to look back for data (if no last run timestamp exists)')
    parser.add_argument('--test-dataset', action='store_true',
                        help='Use test dataset instead of production')
    parser.add_argument('--skip-extraction', action='store_true',
                        help='Skip the extraction step (for testing)')
    parser.add_argument('--skip-analysis', action='store_true',
                        help='Skip the analysis step (for testing)')
    parser.add_argument('--skip-stock-persistence', action='store_true',
                        help='Skip saving stock mentions (for testing)')
    parser.add_argument('--skip-aggregation', action='store_true',
                        help='Skip the aggregation step (for testing)')
    parser.add_argument('--skip-aggregation-persistence', action='store_true',
                        help='Skip saving aggregated data (for testing)')
    
    args = parser.parse_args()
    
    logger.info("Starting Reddit ETL job for stock analysis (Test Runner)")
    
    try:
        # Set up environment
        setup_environment(test_mode=args.test_dataset)
        
        # Check environment variables
        if not check_environment():
            logger.error("Environment check failed, exiting")
            sys.exit(1)
            
        # Set up BigQuery
        setup_bigquery()
        
        # Current time used for all timestamp updates
        current_time = datetime.utcnow()
        logger.info(f"Current time: {current_time}")
        
        # 1. Extract data from BigQuery
        if args.skip_extraction:
            logger.info("Skipping extraction step")
            reddit_data = []
        else:
            reddit_data = extract_reddit_data(
                current_time, 
                hours_back=args.hours_back,
                data_limit=args.data_limit
            )
        
        # 2. Analyze data for stock mentions
        if args.skip_analysis:
            logger.info("Skipping analysis step")
            stock_mentions = []
        else:
            stock_mentions = analyze_stock_mentions(reddit_data, current_time)
        
        # 3. Save stock mentions to BigQuery
        if args.skip_stock_persistence:
            logger.info("Skipping stock mentions persistence step")
        else:
            save_stock_mentions(stock_mentions, current_time)
        
        # 4. Aggregate summaries
        if args.skip_aggregation:
            logger.info("Skipping aggregation step")
            daily_summaries, hourly_summaries, weekly_summaries = [], [], []
        else:
            daily_summaries, hourly_summaries, weekly_summaries = aggregate_summaries(stock_mentions, current_time)
        
        # 5. Save aggregated data
        if args.skip_aggregation_persistence:
            logger.info("Skipping aggregation persistence step")
        else:
            save_aggregated_data(daily_summaries, hourly_summaries, weekly_summaries, current_time)
        
        logger.info("Test run completed successfully")
        
    except Exception as e:
        logger.error(f"Test run failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 