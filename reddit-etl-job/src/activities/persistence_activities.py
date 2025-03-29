import logging
from typing import List, Dict, Any

from temporalio import activity

from src.models.stock_data import StockMention, DailySummary, HourlySummary, WeeklySummary
from src.utils.bigquery_utils import BigQueryManager, DailyBigQueryManager, HourlyBigQueryManager, WeeklyBigQueryManager

logger = logging.getLogger(__name__)

@activity.defn
async def save_stock_mentions_activity(stock_mentions: List[StockMention]) -> int:
    """
    Activity to save stock mentions to BigQuery.
    
    Args:
        stock_mentions: List of stock mention objects
        
    Returns:
        Number of stock mentions saved
    """
    logger.info(f"Starting persistence activity: Saving {len(stock_mentions)} stock mentions to BigQuery")
    
    if not stock_mentions:
        logger.info("No stock mentions to save")
        return 0
    
    # Get BigQuery manager
    bq_manager = BigQueryManager()
    bq_manager.connect()
    bq_manager.setup_tables()
    
    # Convert stock mentions to dicts for BigQuery insertion
    mention_dicts = [mention.to_dict() for mention in stock_mentions]
    
    # Bulk insert to BigQuery
    bq_manager.bulk_insert_stock_mentions(mention_dicts)
    
    logger.info(f"Successfully saved {len(stock_mentions)} stock mentions to BigQuery")
    
    return len(stock_mentions)

@activity.defn
async def save_daily_summaries_activity(daily_summaries: List[DailySummary]) -> int:
    """
    Activity to save daily summaries to BigQuery.
    
    Args:
        daily_summaries: List of daily summary objects
        
    Returns:
        Number of daily summaries saved
    """
    logger.info(f"Starting persistence activity: Saving {len(daily_summaries)} daily summaries to BigQuery")
    
    if not daily_summaries:
        logger.info("No daily summaries to save")
        return 0
    
    # Get specialized manager for daily summaries
    daily_manager = DailyBigQueryManager()
    
    # Use the manager to save all summaries
    saved_count = daily_manager.save_records(daily_summaries)
    
    logger.info(f"Successfully saved {saved_count} daily summaries to BigQuery")
    
    return saved_count

@activity.defn
async def save_hourly_summaries_activity(hourly_summaries: List[HourlySummary]) -> int:
    """
    Activity to save hourly summaries to BigQuery.
    
    Args:
        hourly_summaries: List of hourly summary objects
        
    Returns:
        Number of hourly summaries saved
    """
    logger.info(f"Starting persistence activity: Saving {len(hourly_summaries)} hourly summaries to BigQuery")
    
    if not hourly_summaries:
        logger.info("No hourly summaries to save")
        return 0
    
    # Get specialized manager for hourly summaries
    hourly_manager = HourlyBigQueryManager()
    
    # Use the manager to save all summaries
    saved_count = hourly_manager.save_records(hourly_summaries)
    
    logger.info(f"Successfully saved {saved_count} hourly summaries to BigQuery")
    
    return saved_count

@activity.defn
async def save_weekly_summaries_activity(weekly_summaries: List[WeeklySummary]) -> int:
    """
    Activity to save weekly summaries to BigQuery.
    
    Args:
        weekly_summaries: List of weekly summary objects
        
    Returns:
        Number of weekly summaries saved
    """
    logger.info(f"Starting persistence activity: Saving {len(weekly_summaries)} weekly summaries to BigQuery")
    
    if not weekly_summaries:
        logger.info("No weekly summaries to save")
        return 0
    
    # Get specialized manager for weekly summaries
    weekly_manager = WeeklyBigQueryManager()
    
    # Use the manager to save all summaries
    saved_count = weekly_manager.save_records(weekly_summaries)
    
    logger.info(f"Successfully saved {saved_count} weekly summaries to BigQuery")
    
    return saved_count 