import logging
import json
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.engine import Engine

from src.models.stock_data import StockMention, DailySummary
from src.utils.base_aggregator import BaseAggregator
from src.utils.json_utils import safe_json_loads, merge_count_dictionaries

logger = logging.getLogger(__name__)

class DailyAggregator(BaseAggregator[DailySummary]):
    """
    Aggregates stock mentions by day.
    """
    
    def __init__(self, db_engine: Engine):
        """
        Initialize the daily aggregator.
        
        Args:
            db_engine: SQLAlchemy database engine
        """
        self.db_engine = db_engine
    
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
                weighted_sentiment = np.average(group['sentiment_compound'], weights=weights)
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
            
        updated_summaries = []
        
        with self.db_engine.connect() as connection:
            for summary in summaries:
                date_str = summary.date.strftime('%Y-%m-%d')
                
                # Check if summary already exists
                query = f"""
                SELECT id, mention_count, buy_signals, sell_signals, hold_signals, 
                       news_signals, earnings_signals, technical_signals, options_signals,
                       price_targets, subreddits
                FROM stock_daily_summary 
                WHERE ticker = '{summary.ticker}' AND date::date = '{date_str}'::date
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
                        'subreddits': result[10]
                    }
                    
                    # Merge with new summary
                    merged_summary = self._merge_summaries(summary, existing_data)
                    merged_summary.id = existing_id  # Store ID for updating
                    updated_summaries.append(merged_summary)
                else:
                    # No existing summary, use as is
                    updated_summaries.append(summary)
        
        return updated_summaries
    
    def _merge_summaries(self, summary: DailySummary, existing_data: Dict[str, Any]) -> DailySummary:
        """
        Merge a new summary with existing data.
        
        Args:
            summary: New daily summary
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
        
        # Merge subreddits
        existing_subreddits = safe_json_loads(existing_data['subreddits'], {})
        summary.subreddits = merge_count_dictionaries(summary.subreddits, existing_subreddits)
        
        return summary
    
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