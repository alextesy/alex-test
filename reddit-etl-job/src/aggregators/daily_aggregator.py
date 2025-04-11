import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

from src.models.stock_data import StockMention, DailySummary
from src.utils.base_aggregator import BaseAggregator
from src.utils.json_utils import safe_json_loads, merge_count_dictionaries

logger = logging.getLogger(__name__)

class DailyAggregator(BaseAggregator[DailySummary]):
    """
    Aggregates stock mentions by day.
    """
    
    def __init__(self):

        # Call the base class constructor
        super().__init__()
    
    def aggregate(self, mentions: List[StockMention], incremental: bool = True) -> List[DailySummary]:
        """
        Aggregate stock mentions by day.
        
        Args:
            mentions: List of stock mentions
            incremental: Whether to incrementally update existing summaries
            
        Returns:
            List of daily summaries
        """
        if not mentions:
            logger.info("No stock mentions to aggregate by day")
            return []
        
        # Convert to DataFrame for easier grouping
        df = pd.DataFrame([m.__dict__ for m in mentions])
        
        self._add_time_columns(df)
        
        # Group by ticker and date
        grouped = self._group_data(df)
        
        summaries = []
        
        for (ticker, date), group in grouped:
            # Count mentions
            mention_count = len(group)
            
            # Calculate average sentiment
            avg_sentiment = group['sentiment_compound'].mean()
            
            # Calculate weighted sentiment (weighted by confidence)
            if 'confidence' in group.columns and not group['confidence'].isna().all():
                weights = group['confidence']
                if weights.sum() > 0:
                    weighted_sentiment = np.average(group['sentiment_compound'], weights=weights)
                else:
                    # If all weights are zero, fall back to simple average
                    weighted_sentiment = avg_sentiment
            else:
                weighted_sentiment = avg_sentiment
            
            # Count signals
            buy_signals = sum(group['signals'].apply(lambda x: 'BUY' in x if isinstance(x, list) else False))
            sell_signals = sum(group['signals'].apply(lambda x: 'SELL' in x if isinstance(x, list) else False))
            hold_signals = sum(group['signals'].apply(lambda x: 'HOLD' in x if isinstance(x, list) else False))
            
            news_signals = sum(group['signals'].apply(lambda x: 'NEWS' in x if isinstance(x, list) else False))
            earnings_signals = sum(group['signals'].apply(lambda x: 'EARNINGS' in x if isinstance(x, list) else False))
            technical_signals = sum(group['signals'].apply(lambda x: 'TECHNICAL' in x if isinstance(x, list) else False))
            options_signals = sum(group['signals'].apply(lambda x: 'OPTIONS' in x if isinstance(x, list) else False))
            
            # Extract price targets
            price_targets = {}
            for signals in group['signals']:
                if isinstance(signals, list):
                    for signal in signals:
                        if signal.startswith('PT:'):
                            try:
                                price = float(signal.split(':')[1])
                                price_targets[str(price)] = price_targets.get(str(price), 0) + 1
                            except (ValueError, IndexError):
                                pass
            
            # Average confidence
            avg_confidence = group['confidence'].mean() if 'confidence' in group.columns else 0.0
            
            # Sentiment of high confidence mentions (confidence > 0.7)
            high_conf_mentions = group[group['confidence'] > 0.7] if 'confidence' in group.columns else pd.DataFrame()
            high_conf_sentiment = high_conf_mentions['sentiment_compound'].mean() if not high_conf_mentions.empty else None
            
            # Get top contexts by confidence
            top_contexts = []
            if 'confidence' in group.columns and 'context' in group.columns:
                group_with_context = group[['confidence', 'context', 'sentiment_compound']].sort_values('confidence', ascending=False)
                for _, ctx_row in group_with_context.head(3).iterrows():
                    top_contexts.append({
                        'context': ctx_row['context'],
                        'confidence': float(ctx_row['confidence']),
                        'sentiment': float(ctx_row['sentiment_compound'])
                    })
            
            # Count by subreddit
            subreddit_counts = group['subreddit'].value_counts().to_dict()
            
            # Create summary
            summary = self._process_group((ticker, date), group)
            
            summaries.append(summary)
        
        logger.info(f"Generated {len(summaries)} daily stock summaries")
        
        if incremental:
            summaries = self.merge_with_existing(summaries)
        
        return summaries
    
    def _add_time_columns(self, df: pd.DataFrame) -> None:
        """
        Add date column for daily grouping.
        
        Args:
            df: DataFrame with stock mentions
        """
        # Convert created_at to date for grouping
        df['date'] = pd.to_datetime(df['created_at']).dt.date
    
    def _group_data(self, df: pd.DataFrame):
        """
        Group data by ticker and date.
        
        Args:
            df: DataFrame with stock mentions
            
        Returns:
            DataFrame grouped by ticker and date
        """
        return df.groupby(['ticker', 'date'])
    
    def _process_group(self, group_key: Tuple[str, datetime.date], group: pd.DataFrame) -> DailySummary:
        """
        Process a group of stock mentions to create a daily summary.
        
        Args:
            group_key: Tuple of (ticker, date)
            group: DataFrame with stock mentions in this group
            
        Returns:
            DailySummary object
        """
        ticker, date = group_key
        
        # Calculate common metrics
        metrics = self._calculate_common_metrics(group)
        
        # Create daily summary
        return DailySummary(
            ticker=ticker,
            date=datetime.combine(date, datetime.min.time()),
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
            high_conf_sentiment=metrics['high_conf_sentiment'],
            top_contexts=metrics['top_contexts'],
            subreddits=metrics['subreddits'],
            etl_timestamp=datetime.utcnow()
        )
    
    def merge_with_existing(self, summaries: List[DailySummary]) -> List[DailySummary]:
        """
        Merge new summaries with existing ones in the database.
        
        Args:
            summaries: List of new daily summaries
            
        Returns:
            List of merged summaries
        """
        if not summaries:
            return []
            
        # Skip database merge since we're using BigQuery for storage
        # and don't have a PostgreSQL database configured
        logger.info("Skipping database merge - using direct BigQuery storage instead")
        return summaries
    
    def save_to_database(self, summaries: List[DailySummary]):
        """
        Save daily summaries to the database.
        
        Args:
            summaries: List of daily summaries
        """
        if not summaries:
            return
            
        with self.db_engine.connect() as connection:
            with connection.begin():
                for summary in summaries:
                    # Convert to dict for SQL
                    data = summary.to_dict()
                    
                    # Check if we have an ID (for merging)
                    if hasattr(summary, 'id') and summary.id:
                        # Update existing record
                        query = """
                        UPDATE stock_daily_summary SET
                            mention_count = :mention_count,
                            avg_sentiment = :avg_sentiment,
                            weighted_sentiment = :weighted_sentiment,
                            buy_signals = :buy_signals,
                            sell_signals = :sell_signals,
                            hold_signals = :hold_signals,
                            price_targets = :price_targets,
                            news_signals = :news_signals,
                            earnings_signals = :earnings_signals,
                            technical_signals = :technical_signals,
                            options_signals = :options_signals,
                            avg_confidence = :avg_confidence,
                            high_conf_sentiment = :high_conf_sentiment,
                            top_contexts = :top_contexts,
                            subreddits = :subreddits,
                            etl_timestamp = :etl_timestamp
                        WHERE id = :id
                        """
                        connection.execute(query, {**data, 'id': summary.id})
                    else:
                        # Delete any existing records
                        query = """
                        DELETE FROM stock_daily_summary 
                        WHERE ticker = :ticker AND date::date = :date::date
                        """
                        connection.execute(query, {'ticker': summary.ticker, 'date': summary.date})
                        
                        # Insert new record
                        query = """
                        INSERT INTO stock_daily_summary 
                        (ticker, date, mention_count, avg_sentiment, weighted_sentiment, buy_signals, 
                         sell_signals, hold_signals, price_targets, news_signals, earnings_signals,
                         technical_signals, options_signals, avg_confidence, high_conf_sentiment,
                         top_contexts, subreddits, etl_timestamp) 
                        VALUES (:ticker, :date, :mention_count, :avg_sentiment, :weighted_sentiment, :buy_signals,
                                :sell_signals, :hold_signals, :price_targets, :news_signals, :earnings_signals,
                                :technical_signals, :options_signals, :avg_confidence, :high_conf_sentiment,
                                :top_contexts, :subreddits, :etl_timestamp)
                        """
                        connection.execute(query, data)
        
        logger.info(f"Saved {len(summaries)} daily summaries to database") 