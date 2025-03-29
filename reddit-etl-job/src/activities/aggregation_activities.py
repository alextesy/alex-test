import logging
from typing import List
import sqlalchemy

from temporalio import activity

from src.models.stock_data import StockMention, DailySummary, HourlySummary, WeeklySummary
from src.aggregators.daily_aggregator import DailyAggregator
from src.aggregators.hourly_aggregator import HourlyAggregator
from src.aggregators.weekly_aggregator import WeeklyAggregator
from src.utils.db_utils import DBManager

logger = logging.getLogger(__name__)

@activity.defn
async def aggregate_daily_summaries_activity(
    stock_mentions: List[StockMention]
) -> List[DailySummary]:
    """
    Activity to aggregate stock mentions into daily summaries.
    
    Args:
        stock_mentions: List of stock mention objects
        
    Returns:
        List of daily summary objects
    """
    logger.info(f"Starting aggregation activity: Creating daily summaries from {len(stock_mentions)} stock mentions")
    
    # Get database connection
    db_manager = DBManager()
    engine = db_manager.connect()
    
    # Create aggregator and process data
    aggregator = DailyAggregator(engine)
    daily_summaries = aggregator.aggregate(stock_mentions, incremental=True)
    
    logger.info(f"Generated {len(daily_summaries)} daily stock summaries")
    
    return daily_summaries

@activity.defn
async def aggregate_hourly_summaries_activity(
    stock_mentions: List[StockMention]
) -> List[HourlySummary]:
    """
    Activity to aggregate stock mentions into hourly summaries.
    
    Args:
        stock_mentions: List of stock mention objects
        
    Returns:
        List of hourly summary objects
    """
    logger.info(f"Starting aggregation activity: Creating hourly summaries from {len(stock_mentions)} stock mentions")
    
    # Get database connection
    db_manager = DBManager()
    engine = db_manager.connect()
    
    # Create aggregator and process data
    aggregator = HourlyAggregator(engine)
    hourly_summaries = aggregator.aggregate(stock_mentions, incremental=True)
    
    logger.info(f"Generated {len(hourly_summaries)} hourly stock summaries")
    
    return hourly_summaries

@activity.defn
async def aggregate_weekly_summaries_activity(
    stock_mentions: List[StockMention]
) -> List[WeeklySummary]:
    """
    Activity to aggregate stock mentions into weekly summaries.
    
    Args:
        stock_mentions: List of stock mention objects
        
    Returns:
        List of weekly summary objects
    """
    logger.info(f"Starting aggregation activity: Creating weekly summaries from {len(stock_mentions)} stock mentions")
    
    # Get database connection
    db_manager = DBManager()
    engine = db_manager.connect()
    
    # Create aggregator and process data
    aggregator = WeeklyAggregator(engine)
    weekly_summaries = aggregator.aggregate(stock_mentions, incremental=True)
    
    logger.info(f"Generated {len(weekly_summaries)} weekly stock summaries")
    
    return weekly_summaries 