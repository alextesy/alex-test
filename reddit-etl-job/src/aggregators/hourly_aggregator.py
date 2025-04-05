import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

from sqlalchemy.engine import Engine

from src.models.stock_data import StockMention, HourlySummary
from src.utils.base_aggregator import BaseAggregator
from src.utils.json_utils import safe_json_loads, merge_count_dictionaries

logger = logging.getLogger(__name__)

class HourlyAggregator(BaseAggregator[HourlySummary]):
    """
    Aggregates stock mentions by hour.
    """
    
    def __init__(self, db_engine=None):
        """
        Initialize the hourly aggregator.
        
        Args:
            db_engine: Optional SQLAlchemy database engine (not used)
        """
        # Call the base class constructor
        super().__init__(db_engine)
    
    def _add_time_columns(self, df: pd.DataFrame) -> None:
        """
        Add hour column for hourly grouping.
        
        Args:
            df: DataFrame with stock mentions
        """
        # Convert created_at to hourly buckets
        df['created_at_dt'] = pd.to_datetime(df['created_at'])
        df['hour_start'] = df['created_at_dt'].dt.floor('H')
    
    def _group_data(self, df: pd.DataFrame):
        """
        Group data by ticker and hour.
        
        Args:
            df: DataFrame with stock mentions
            
        Returns:
            DataFrame grouped by ticker and hour
        """
        return df.groupby(['ticker', 'hour_start'])
    
    def _process_group(self, group_key: Tuple[str, datetime], group: pd.DataFrame) -> HourlySummary:
        """
        Process a group of stock mentions to create an hourly summary.
        
        Args:
            group_key: Tuple of (ticker, hour_start)
            group: DataFrame with stock mentions in this group
            
        Returns:
            HourlySummary object
        """
        ticker, hour_start = group_key
        
        # Calculate common metrics
        metrics = self._calculate_common_metrics(group)
        
        # Create hourly summary
        return HourlySummary(
            ticker=ticker,
            hour_start=hour_start,
            mention_count=metrics['mention_count'],
            avg_sentiment=metrics['avg_sentiment'],
            weighted_sentiment=metrics['weighted_sentiment'],
            buy_signals=metrics['buy_signals'],
            sell_signals=metrics['sell_signals'],
            hold_signals=metrics['hold_signals'],
            avg_confidence=metrics['avg_confidence'],
            subreddits=metrics['subreddits'],
            etl_timestamp=datetime.utcnow()
        )
    
    def merge_with_existing(self, summaries: List[HourlySummary]) -> List[HourlySummary]:
        """
        Merge new summaries with existing ones in the database.
        
        Args:
            summaries: List of new hourly summaries
            
        Returns:
            List of merged summaries
        """
        if not summaries:
            return []
            
        # Skip database merge since we're using BigQuery for storage
        # and don't have a PostgreSQL database configured
        logger.info("Skipping database merge - using direct BigQuery storage instead")
        return summaries
    
    def _merge_summaries(self, summary: HourlySummary, existing_data: Dict[str, Any]) -> HourlySummary:
        """
        Merge a new summary with existing data.
        
        Args:
            summary: New hourly summary
            existing_data: Existing summary data from the database
            
        Returns:
            Merged summary
        """
        # Update counts
        summary.mention_count += existing_data['mention_count']
        summary.buy_signals += existing_data['buy_signals']
        summary.sell_signals += existing_data['sell_signals']
        summary.hold_signals += existing_data['hold_signals']
        
        # Merge subreddits
        existing_subreddits = safe_json_loads(existing_data['subreddits'], {})
        summary.subreddits = merge_count_dictionaries(summary.subreddits, existing_subreddits)
        
        return summary 