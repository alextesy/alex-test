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
            
        updated_summaries = []
        
        with self.db_engine.connect() as connection:
            for summary in summaries:
                week_str = summary.week_start.strftime('%Y-%m-%d')
                
                # Check if summary already exists
                query = f"""
                SELECT id, mention_count, buy_signals, sell_signals, hold_signals, 
                       news_signals, earnings_signals, technical_signals, options_signals,
                       price_targets, daily_breakdown, subreddits
                FROM stock_weekly_summary 
                WHERE ticker = '{summary.ticker}' AND week_start::date = '{week_str}'::date
                """
                
                result = connection.execute(query).fetchone()
                
                if result:
                    # Get existing summary data
                    existing_id = result[0]
                    existing_data = {
                        'mention_count': result[1],
                        'buy_signals': result[2],
                        'sell_signals': result[3],
                        'hold_signals': result[4],
                        'news_signals': result[5],
                        'earnings_signals': result[6],
                        'technical_signals': result[7],
                        'options_signals': result[8],
                        'price_targets': result[9],
                        'daily_breakdown': result[10],
                        'subreddits': result[11]
                    }
                    
                    # Merge with new summary
                    merged_summary = self._merge_summaries(summary, existing_data)
                    merged_summary.id = existing_id  # Store ID for updating
                    updated_summaries.append(merged_summary)
                else:
                    # No existing summary, use as is
                    updated_summaries.append(summary)
        
        return updated_summaries
    
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