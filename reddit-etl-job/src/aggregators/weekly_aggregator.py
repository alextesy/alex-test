import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

from sqlalchemy.engine import Engine

from src.models.stock_data import StockMention, WeeklySummary
from src.utils.base_aggregator import BaseAggregator
from src.utils.json_utils import safe_json_loads, merge_count_dictionaries

logger = logging.getLogger(__name__)

class WeeklyAggregator(BaseAggregator[WeeklySummary]):
    """
    Aggregates stock mentions by week.
    """
    
    def __init__(self, db_engine=None):
        """
        Initialize the weekly aggregator.
        
        Args:
            db_engine: Optional SQLAlchemy database engine (not used)
        """
        # Call the base class constructor
        super().__init__(db_engine)
        
    def _add_time_columns(self, df: pd.DataFrame) -> None:
        """
        Add week column for weekly grouping.
        
        Args:
            df: DataFrame with stock mentions
        """
        # Convert created_at to week
        df['created_at_dt'] = pd.to_datetime(df['created_at'])
        # Get the start of the week (Monday)
        df['week_start'] = df['created_at_dt'] - pd.to_timedelta(df['created_at_dt'].dt.dayofweek, unit='D')
        df['week_start'] = df['week_start'].dt.floor('D')  # Floor to start of day
    
    def _group_data(self, df: pd.DataFrame):
        """
        Group data by ticker and week.
        
        Args:
            df: DataFrame with stock mentions
            
        Returns:
            DataFrame grouped by ticker and week
        """
        return df.groupby(['ticker', 'week_start'])
    
    def _process_group(self, group_key: Tuple[str, datetime], group: pd.DataFrame) -> WeeklySummary:
        """
        Process a group of stock mentions to create a weekly summary.
        
        Args:
            group_key: Tuple of (ticker, week_start)
            group: DataFrame with stock mentions in this group
            
        Returns:
            WeeklySummary object
        """
        ticker, week_start = group_key
        
        # Calculate common metrics
        metrics = self._calculate_common_metrics(group)
        
        # Add daily breakdown
        daily_breakdown = {}
        if 'created_at_dt' in group.columns:
            daily_counts = group.groupby(group['created_at_dt'].dt.date).size()
            daily_breakdown = daily_counts.to_dict()
            # Convert date objects to strings for JSON serialization
            daily_breakdown = {str(date): count for date, count in daily_breakdown.items()}
        
        # Create weekly summary
        return WeeklySummary(
            ticker=ticker,
            week_start=week_start,
            mention_count=metrics['mention_count'],
            avg_sentiment=metrics['avg_sentiment'],
            weighted_sentiment=metrics['weighted_sentiment'],
            buy_signals=metrics['buy_signals'],
            sell_signals=metrics['sell_signals'],
            hold_signals=metrics['hold_signals'],
            price_targets=metrics['price_targets'],
            news_signals=metrics['news_signals'],
            earnings_signals=metrics['earnings_signals'],
            technical_signals=metrics['technical_signals'],
            options_signals=metrics['options_signals'],
            avg_confidence=metrics['avg_confidence'],
            daily_breakdown=daily_breakdown,
            subreddits=metrics['subreddits'],
            etl_timestamp=datetime.utcnow()
        )
    
    def merge_with_existing(self, summaries: List[WeeklySummary]) -> List[WeeklySummary]:
        """
        Merge new summaries with existing ones in the database.
        
        Args:
            summaries: List of new weekly summaries
            
        Returns:
            List of merged summaries
        """
        if not summaries:
            return []
            
        # Skip database merge since we're using BigQuery for storage
        # and don't have a PostgreSQL database configured
        logger.info("Skipping database merge - using direct BigQuery storage instead")
        return summaries
    
    def _merge_summaries(self, summary: WeeklySummary, existing_data: Dict[str, Any]) -> WeeklySummary:
        """
        Merge a new summary with existing data.
        
        Args:
            summary: New weekly summary
            existing_data: Existing summary data from the database
            
        Returns:
            Merged summary
        """
        # Update counts
        summary.mention_count += existing_data['mention_count']
        summary.buy_signals += existing_data['buy_signals']
        summary.sell_signals += existing_data['sell_signals']
        summary.hold_signals += existing_data['hold_signals']
        summary.news_signals += existing_data['news_signals']
        summary.earnings_signals += existing_data['earnings_signals']
        summary.technical_signals += existing_data['technical_signals']
        summary.options_signals += existing_data['options_signals']
        
        # Merge price targets
        existing_price_targets = safe_json_loads(existing_data['price_targets'], {})
        summary.price_targets = merge_count_dictionaries(summary.price_targets, existing_price_targets)
        
        # Merge daily breakdown
        existing_breakdown = safe_json_loads(existing_data['daily_breakdown'], {})
        summary.daily_breakdown = merge_count_dictionaries(summary.daily_breakdown, existing_breakdown)
        
        # Merge subreddits
        existing_subreddits = safe_json_loads(existing_data['subreddits'], {})
        summary.subreddits = merge_count_dictionaries(summary.subreddits, existing_subreddits)
        
        return summary 