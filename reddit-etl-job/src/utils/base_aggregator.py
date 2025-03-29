import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Any, Optional, TypeVar, Generic, Type

from sqlalchemy.engine import Engine

from src.models.stock_data import StockMention

# Type variable for the aggregation result type
R = TypeVar('R')

logger = logging.getLogger(__name__)

class BaseAggregator(Generic[R]):
    """
    Base class for all aggregators that process stock mentions.
    """
    
    def __init__(self, db_engine: Engine):
        """
        Initialize the aggregator.
        
        Args:
            db_engine: SQLAlchemy database engine
        """
        self.db_engine = db_engine
    
    def aggregate(self, mentions: List[StockMention], incremental: bool = True) -> List[R]:
        """
        Aggregate stock mentions.
        
        Args:
            mentions: List of stock mentions
            incremental: Whether to incrementally update existing summaries
            
        Returns:
            List of aggregation results
        """
        if not mentions:
            logger.info(f"No stock mentions to aggregate for {self.__class__.__name__}")
            return []
        
        # Convert to DataFrame for easier grouping
        df = pd.DataFrame([m.__dict__ for m in mentions])
        
        # Add time-based columns for grouping
        self._add_time_columns(df)
        
        # Group by ticker and time
        grouped = self._group_data(df)
        
        summaries = []
        
        for group_key, group in grouped:
            # Process each group to create a summary
            summary = self._process_group(group_key, group)
            summaries.append(summary)
        
        logger.info(f"Generated {len(summaries)} summaries using {self.__class__.__name__}")
        
        if incremental:
            summaries = self.merge_with_existing(summaries)
        
        return summaries
    
    def _add_time_columns(self, df: pd.DataFrame) -> None:
        """
        Add time-based columns to the DataFrame for grouping.
        Should be implemented by subclasses.
        
        Args:
            df: DataFrame with stock mentions
        """
        raise NotImplementedError("Subclasses must implement _add_time_columns")
    
    def _group_data(self, df: pd.DataFrame):
        """
        Group data based on ticker and time columns.
        Should be implemented by subclasses.
        
        Args:
            df: DataFrame with stock mentions
            
        Returns:
            Grouped pandas object
        """
        raise NotImplementedError("Subclasses must implement _group_data")
    
    def _process_group(self, group_key, group) -> R:
        """
        Process a group of stock mentions to create a summary.
        Should be implemented by subclasses.
        
        Args:
            group_key: Key for the group (e.g., (ticker, date))
            group: DataFrame with stock mentions in this group
            
        Returns:
            Summary object
        """
        raise NotImplementedError("Subclasses must implement _process_group")
    
    def merge_with_existing(self, summaries: List[R]) -> List[R]:
        """
        Merge new summaries with existing ones in the database.
        Should be implemented by subclasses.
        
        Args:
            summaries: List of new summaries
            
        Returns:
            List of merged summaries
        """
        raise NotImplementedError("Subclasses must implement merge_with_existing")
    
    def _calculate_common_metrics(self, group: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate common metrics from a group.
        
        Args:
            group: DataFrame with stock mentions in a group
            
        Returns:
            Dictionary with common metrics
        """
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
        signals_metrics = self._count_signals(group)
        
        # Average confidence
        avg_confidence = group['confidence'].mean() if 'confidence' in group.columns else 0.0
        
        # Sentiment of high confidence mentions (confidence > 0.7)
        high_conf_mentions = group[group['confidence'] > 0.7] if 'confidence' in group.columns else pd.DataFrame()
        high_conf_sentiment = high_conf_mentions['sentiment_compound'].mean() if not high_conf_mentions.empty else None
        
        # Count by subreddit
        subreddit_counts = group['subreddit'].value_counts().to_dict()
        
        # Combine all metrics
        return {
            'mention_count': mention_count,
            'avg_sentiment': float(avg_sentiment),
            'weighted_sentiment': float(weighted_sentiment),
            'avg_confidence': float(avg_confidence),
            'high_conf_sentiment': float(high_conf_sentiment) if high_conf_sentiment is not None else None,
            'subreddits': subreddit_counts,
            **signals_metrics
        }
    
    def _count_signals(self, group: pd.DataFrame) -> Dict[str, Any]:
        """
        Count different types of signals in a group.
        
        Args:
            group: DataFrame with stock mentions
            
        Returns:
            Dictionary with signal counts
        """
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
        
        return {
            'buy_signals': int(buy_signals),
            'sell_signals': int(sell_signals),
            'hold_signals': int(hold_signals),
            'news_signals': int(news_signals),
            'earnings_signals': int(earnings_signals),
            'technical_signals': int(technical_signals),
            'options_signals': int(options_signals),
            'price_targets': price_targets,
            'top_contexts': top_contexts
        } 