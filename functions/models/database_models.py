from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Table, Boolean, Enum
from sqlalchemy.orm import relationship
import enum
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class MessageType(str, enum.Enum):
    TWEET = 'tweet'
    REDDIT_POST = 'reddit_post'
    REDDIT_COMMENT = 'reddit_comment'
    CNBC_ARTICLE = 'cnbc_article'

# Association table for many-to-many relationship between processed messages and stocks
message_stocks = Table(
    'message_stocks',
    Base.metadata,
    Column('message_id', String, ForeignKey('processed_messages.id')),
    Column('stock_symbol', String, ForeignKey('stocks.symbol'))
)

class RawMessage(Base):
    __tablename__ = "raw_messages"

    id = Column(String, primary_key=True)
    content = Column(String)
    author = Column(String)
    timestamp = Column(Float)
    url = Column(String)
    score = Column(Integer)
    message_type = Column(Enum(MessageType))  # Type of the message (tweet, reddit_post, etc.)
    raw_data = Column(String)  # JSON string of all raw data
    processed = Column(Boolean, default=False)  # Flag to track processing status
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Platform-specific fields
    # Twitter fields
    retweet_count = Column(Integer, nullable=True)
    favorite_count = Column(Integer, nullable=True)
    
    # Reddit fields
    title = Column(String, nullable=True)
    selftext = Column(String, nullable=True)
    num_comments = Column(Integer, nullable=True)
    subreddit = Column(String, nullable=True)
    parent_id = Column(String, nullable=True)
    depth = Column(Integer, nullable=True)
    
    # CNBC fields
    summary = Column(String, nullable=True)
    category = Column(String, nullable=True)
    author_title = Column(String, nullable=True)

class ProcessedMessage(Base):
    __tablename__ = "processed_messages"

    id = Column(String, primary_key=True)
    raw_message_id = Column(String, ForeignKey('raw_messages.id'))
    content = Column(String)
    author = Column(String)
    timestamp = Column(Float)
    created_at = Column(DateTime)  # When the post/comment was written
    url = Column(String)
    score = Column(Integer)
    message_type = Column(Enum(MessageType))  # Type of the message (tweet, reddit_post, etc.)
    sentiment = Column(Float)
    processed_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship with raw message
    raw_message = relationship("RawMessage", backref="processed_messages")
    
    # Platform-specific fields
    title = Column(String, nullable=True)
    retweet_count = Column(Integer, nullable=True)
    favorite_count = Column(Integer, nullable=True)
    num_comments = Column(Integer, nullable=True)
    subreddit = Column(String, nullable=True)
    selftext = Column(String, nullable=True)
    
    # Comment-specific fields
    parent_id = Column(String, nullable=True)
    depth = Column(Integer, nullable=True)
    
    # CNBC-specific fields
    summary = Column(String, nullable=True)
    category = Column(String, nullable=True)
    author_title = Column(String, nullable=True)
    
    # Relationship with stocks
    stocks = relationship("DBStock", secondary=message_stocks, back_populates="messages")

class DBStock(Base):
    __tablename__ = "stocks"

    symbol = Column(String, primary_key=True)
    name = Column(String)
    sector = Column(String, nullable=True)
    exchange = Column(String, nullable=True)
    last_updated = Column(DateTime, default=datetime.utcnow)
    
    # Relationship with messages
    messages = relationship("ProcessedMessage", secondary=message_stocks, back_populates="stocks") 