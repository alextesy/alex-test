from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Set
from sqlalchemy.orm import Session
from .database_models import ProcessedMessage, DBStock
from .stock import Stock

@dataclass
class Message(ABC):
    """Base class for all social media messages"""
    id: str
    content: str
    author: str
    timestamp: float
    url: str
    score: int
    platform: str
    source: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    mentioned_stocks: Set[Stock] = field(default_factory=set)
    sentiment: float = 0.0
    message_type: str = field(init=False)  # Will be set by child classes
    
    def to_db_model(self, db: Session) -> ProcessedMessage:
        """Convert to database model"""
        db_message = ProcessedMessage(
            id=self.id,
            content=self.content,
            author=self.author,
            timestamp=self.timestamp,
            created_at=self.created_at,
            url=self.url,
            score=self.score,
            platform=self.platform,
            source=self.source,
            sentiment=self.sentiment,
            message_type=self.message_type
        )
        
        # Add platform-specific fields
        if isinstance(self, Tweet):
            db_message.retweet_count = self.retweet_count
            db_message.favorite_count = self.favorite_count
        elif isinstance(self, RedditPost):
            db_message.title = self.title
            db_message.selftext = self.selftext
            db_message.num_comments = self.num_comments
            db_message.subreddit = self.subreddit
            
        # Link stocks
        for stock in self.mentioned_stocks:
            db_stock = db.query(Stock).filter(Stock.symbol == stock.symbol).first()
            if not db_stock:
                db_stock = stock.to_db_model(db)
                db.add(db_stock)
            db_message.stocks.append(db_stock)
            
        return db_message

@dataclass
class Tweet(Message):
    """Twitter message model"""
    title: str = ''  # Twitter doesn't have titles
    retweet_count: int = 0    # Added default value
    favorite_count: int = 0    # Added default value
    
    def __post_init__(self):
        self.message_type = 'tweet'
    
    @property
    def comments_count(self) -> int:
        """Alias for retweet_count to maintain consistency with other platforms"""
        return self.retweet_count

@dataclass
class RedditPost(Message):
    """Reddit post model"""
    title: str = ''
    selftext: str = ''
    num_comments: int = 0
    subreddit: str = ''
    
    def __post_init__(self):
        self.message_type = 'reddit_post'
    
    @property
    def comments_count(self) -> int:
        """Alias for num_comments to maintain consistency with other platforms"""
        return self.num_comments

@dataclass
class RedditComment(RedditPost):
    """Reddit comment model"""
    parent_id: str = ''  # ID of the parent post or comment
    depth: int = 0      # Nesting level of the comment
    title: str = None   # Comments don't have titles
    
    def __post_init__(self):
        self.message_type = 'reddit_comment'
    
    @property
    def comments_count(self) -> int:
        """Number of replies to this comment"""
        return self.num_comments

    def to_db_model(self, db: Session) -> ProcessedMessage:
        """Convert to database model with comment-specific fields"""
        db_message = super().to_db_model(db)
        db_message.parent_id = self.parent_id
        db_message.depth = self.depth
        return db_message