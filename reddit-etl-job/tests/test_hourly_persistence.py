#!/usr/bin/env python3
"""
Test script for hourly aggregation and persistence.

This script creates test stock mentions and tests the hourly aggregation
and persistence to validate the timestamp format fix.
"""
import logging
import os
import sys
import asyncio
from datetime import datetime, timedelta
import random
import argparse

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

def setup_environment(test_mode=True):
    """Set up the environment for testing."""
    # Set environment variables for testing
    if 'GOOGLE_CLOUD_PROJECT_ID' not in os.environ:
        os.environ['GOOGLE_CLOUD_PROJECT_ID'] = 'alex-stocks'
    
    if 'BIGQUERY_DATASET' not in os.environ:
        if test_mode:
            os.environ['BIGQUERY_DATASET'] = 'reddit_stock_test'
        else:
            os.environ['BIGQUERY_DATASET'] = 'reddit_stocks'

        
    # Set up separate Firestore collection for test state management
    if test_mode:
        # Create a test state collection to avoid affecting production timestamps
        os.environ['FIRESTORE_STATE_COLLECTION'] = 'pipeline_state_test'
        logger.info("Using test state collection: pipeline_state_test")
    else:
        # Use the production state collection
        os.environ['FIRESTORE_STATE_COLLECTION'] = 'pipeline_state'
        logger.info("Using production state collection: pipeline_state")

def setup_bigquery():
    """Set up BigQuery tables."""
    from src.utils.bigquery_utils import BigQueryManager
    bq_manager = BigQueryManager()
    bq_manager.setup_tables()
    return bq_manager

def create_test_stock_mentions(num_mentions=20, tickers=None, hours_back=24):
    """
    Create test stock mentions for testing hourly aggregation.
    
    Args:
        num_mentions: Number of stock mentions to create
        tickers: List of tickers to use (defaults to ['AAPL', 'MSFT', 'TSLA', 'GME', 'AMC'])
        hours_back: Hours to spread the mentions across
        
    Returns:
        List of StockMention objects
    """
    from src.models.stock_data import StockMention
    
    if tickers is None:
        tickers = ['AAPL', 'MSFT', 'TSLA', 'GME', 'AMC']
        
    subreddits = ['wallstreetbets', 'stocks', 'investing']
    
    # Create test mentions
    current_time = datetime.utcnow()
    mentions = []
    
    for i in range(num_mentions):
        # Randomly select ticker and subreddit
        ticker = random.choice(tickers)
        subreddit = random.choice(subreddits)
        
        # Random timestamp within the last hours_back hours
        hours_offset = random.uniform(0, hours_back)
        created_at = current_time - timedelta(hours=hours_offset)
        
        # Create stock mention
        mention = StockMention(
            id=f"test_{i}",
            message_id=f"message_{i}",
            ticker=ticker,
            text=f"Test mention of {ticker} with bullish sentiment",
            created_at=created_at,
            sentiment=random.uniform(-1.0, 1.0),
            confidence=random.uniform(0.5, 1.0),
            subreddit=subreddit,
            score=random.randint(1, 100),
            signal="BUY" if random.random() > 0.5 else "SELL",
            post_title=f"Test post about {ticker}"
        )
        
        mentions.append(mention)
    
    logger.info(f"Created {len(mentions)} test stock mentions across {len(tickers)} tickers")
    return mentions

def test_hourly_aggregation(stock_mentions):
    """
    Test hourly aggregation with the created stock mentions.
    
    Args:
        stock_mentions: List of stock mentions
        
    Returns:
        List of hourly summaries
    """
    from src.activities.aggregation_activities import aggregate_hourly_summaries_activity
    
    logger.info(f"Aggregating {len(stock_mentions)} stock mentions into hourly summaries")
    
    hourly_summaries = run_async_activity(
        aggregate_hourly_summaries_activity,
        stock_mentions
    )
    
    logger.info(f"Generated {len(hourly_summaries)} hourly summaries")
    
    # Log hour_start values for inspection
    for summary in hourly_summaries:
        logger.info(f"Ticker: {summary.ticker}, Hour start: {summary.hour_start}, Mentions: {summary.mention_count}")
    
    return hourly_summaries

def test_hourly_persistence(hourly_summaries):
    """
    Test saving hourly summaries to BigQuery.
    
    Args:
        hourly_summaries: List of hourly summaries
        
    Returns:
        Number of hourly summaries saved
    """
    from src.activities.persistence_activities import save_hourly_summaries_activity
    
    logger.info(f"Saving {len(hourly_summaries)} hourly summaries to BigQuery")
    
    result = run_async_activity(save_hourly_summaries_activity, hourly_summaries)
    
    logger.info(f"Successfully saved {result} hourly summaries to BigQuery")
    return result

def main():
    """Main entry point for the test script."""
    parser = argparse.ArgumentParser(description='Test hourly aggregation and persistence')
    parser.add_argument('--production', action='store_true',
                        help='Use production dataset and state collection (WARNING: affects production)')
    parser.add_argument('--num-mentions', type=int, default=50,
                        help='Number of test stock mentions to create')
    parser.add_argument('--hours-back', type=int, default=24,
                        help='Hours to spread the test mentions across')
    
    args = parser.parse_args()
    
    try:
        # Set up environment with test dataset and state collection by default
        setup_environment(test_mode=not args.production)
        
        if args.production:
            logger.warning("WARNING: Using production dataset and state collection!")
        else:
            logger.info("Using test dataset and state collection (safe mode)")
        
        # Set up BigQuery
        setup_bigquery()
        
        # Create test stock mentions
        stock_mentions = create_test_stock_mentions(
            num_mentions=args.num_mentions, 
            hours_back=args.hours_back
        )
        
        # Test hourly aggregation
        hourly_summaries = test_hourly_aggregation(stock_mentions)
        
        # Test hourly persistence
        test_hourly_persistence(hourly_summaries)
        
        logger.info("Hourly aggregation and persistence test completed successfully")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 