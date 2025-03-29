from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Optional

from src.utils.json_utils import safe_json_dumps


@dataclass
class StockMention:
    """
    Represents a mention of a stock ticker in a Reddit post or comment.
    """
    message_id: str
    ticker: str
    author: str
    created_at: datetime
    subreddit: str
    url: str
    score: int
    message_type: str
    sentiment_compound: float
    sentiment_positive: float
    sentiment_negative: float
    sentiment_neutral: float
    signals: List[str] = field(default_factory=list)
    context: str = ""
    confidence: float = 0.0
    etl_timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to a dictionary representation, suitable for database insertion.
        """
        return {
            'message_id': self.message_id,
            'ticker': self.ticker,
            'author': self.author,
            'created_at': self.created_at,
            'subreddit': self.subreddit,
            'url': self.url,
            'score': self.score,
            'message_type': self.message_type,
            'sentiment_compound': float(self.sentiment_compound),
            'sentiment_positive': float(self.sentiment_positive),
            'sentiment_negative': float(self.sentiment_negative),
            'sentiment_neutral': float(self.sentiment_neutral),
            'signals': safe_json_dumps(self.signals),
            'context': self.context,
            'confidence': float(self.confidence),
            'etl_timestamp': self.etl_timestamp
        }


@dataclass
class DailySummary:
    """
    Daily aggregation of stock mentions.
    """
    ticker: str
    date: datetime
    mention_count: int
    avg_sentiment: float
    weighted_sentiment: float
    buy_signals: int = 0
    sell_signals: int = 0
    hold_signals: int = 0
    price_targets: Dict[str, int] = field(default_factory=dict)
    news_signals: int = 0
    earnings_signals: int = 0
    technical_signals: int = 0
    options_signals: int = 0
    avg_confidence: float = 0.0
    high_conf_sentiment: Optional[float] = None
    top_contexts: List[Dict[str, Any]] = field(default_factory=list)
    subreddits: Dict[str, int] = field(default_factory=dict)
    etl_timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to a dictionary representation, suitable for database insertion.
        """
        return {
            'ticker': self.ticker,
            'date': self.date,
            'mention_count': self.mention_count,
            'avg_sentiment': float(self.avg_sentiment),
            'weighted_sentiment': float(self.weighted_sentiment),
            'buy_signals': int(self.buy_signals),
            'sell_signals': int(self.sell_signals),
            'hold_signals': int(self.hold_signals),
            'price_targets': safe_json_dumps(self.price_targets),
            'news_signals': int(self.news_signals),
            'earnings_signals': int(self.earnings_signals),
            'technical_signals': int(self.technical_signals),
            'options_signals': int(self.options_signals),
            'avg_confidence': float(self.avg_confidence),
            'high_conf_sentiment': float(self.high_conf_sentiment) if self.high_conf_sentiment is not None else None,
            'top_contexts': safe_json_dumps(self.top_contexts),
            'subreddits': safe_json_dumps(self.subreddits),
            'etl_timestamp': self.etl_timestamp
        }


@dataclass
class HourlySummary:
    """
    Hourly aggregation of stock mentions.
    """
    ticker: str
    hour_start: datetime
    mention_count: int
    avg_sentiment: float
    weighted_sentiment: float
    buy_signals: int = 0
    sell_signals: int = 0
    hold_signals: int = 0
    avg_confidence: float = 0.0
    subreddits: Dict[str, int] = field(default_factory=dict)
    etl_timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to a dictionary representation, suitable for database insertion.
        """
        return {
            'ticker': self.ticker,
            'hour_start': self.hour_start,
            'mention_count': self.mention_count,
            'avg_sentiment': float(self.avg_sentiment),
            'weighted_sentiment': float(self.weighted_sentiment),
            'buy_signals': int(self.buy_signals),
            'sell_signals': int(self.sell_signals),
            'hold_signals': int(self.hold_signals),
            'avg_confidence': float(self.avg_confidence),
            'subreddits': safe_json_dumps(self.subreddits),
            'etl_timestamp': self.etl_timestamp
        }


@dataclass
class WeeklySummary:
    """
    Weekly aggregation of stock mentions.
    """
    ticker: str
    week_start: datetime
    mention_count: int
    avg_sentiment: float
    weighted_sentiment: float
    buy_signals: int = 0
    sell_signals: int = 0
    hold_signals: int = 0
    price_targets: Dict[str, int] = field(default_factory=dict)
    news_signals: int = 0
    earnings_signals: int = 0
    technical_signals: int = 0
    options_signals: int = 0
    avg_confidence: float = 0.0
    daily_breakdown: Dict[str, int] = field(default_factory=dict)
    subreddits: Dict[str, int] = field(default_factory=dict)
    etl_timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to a dictionary representation, suitable for database insertion.
        """
        return {
            'ticker': self.ticker,
            'week_start': self.week_start,
            'mention_count': self.mention_count,
            'avg_sentiment': float(self.avg_sentiment),
            'weighted_sentiment': float(self.weighted_sentiment),
            'buy_signals': int(self.buy_signals),
            'sell_signals': int(self.sell_signals),
            'hold_signals': int(self.hold_signals),
            'price_targets': safe_json_dumps(self.price_targets),
            'news_signals': int(self.news_signals),
            'earnings_signals': int(self.earnings_signals),
            'technical_signals': int(self.technical_signals),
            'options_signals': int(self.options_signals),
            'avg_confidence': float(self.avg_confidence),
            'daily_breakdown': safe_json_dumps(self.daily_breakdown),
            'subreddits': safe_json_dumps(self.subreddits),
            'etl_timestamp': self.etl_timestamp
        } 