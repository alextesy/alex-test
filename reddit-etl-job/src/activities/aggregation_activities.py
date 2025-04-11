import logging
from typing import List

from temporalio import activity

from src.models.stock_data import StockMention, DailySummary, HourlySummary, WeeklySummary
from src.aggregators.daily_aggregator import DailyAggregator
from src.aggregators.hourly_aggregator import HourlyAggregator
from src.aggregators.weekly_aggregator import WeeklyAggregator

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
    
    
    # Create aggregator and process data
    aggregator = DailyAggregator()
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
    

    
    # Create aggregator and process data
    aggregator = HourlyAggregator()
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

    
    # Create aggregator and process data
    aggregator = WeeklyAggregator()
    weekly_summaries = aggregator.aggregate(stock_mentions, incremental=True)
    
    logger.info(f"Generated {len(weekly_summaries)} weekly stock summaries")
    
    return weekly_summaries 