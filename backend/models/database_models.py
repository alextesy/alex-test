from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Table
from sqlalchemy.orm import relationship
from db.database import Base

# Association table for many-to-many relationship between messages and stocks
message_stocks = Table(
    'message_stocks',
    Base.metadata,
    Column('message_id', String, ForeignKey('messages.id')),
    Column('stock_symbol', String, ForeignKey('stocks.symbol'))
)

class DBMessage(Base):
    __tablename__ = "messages"

    id = Column(String, primary_key=True)
    content = Column(String)
    author = Column(String)
    timestamp = Column(Float)
    url = Column(String)
    score = Column(Integer)
    platform = Column(String)
    source = Column(String)
    sentiment = Column(Float)
    
    # Platform-specific fields
    title = Column(String, nullable=True)
    retweet_count = Column(Integer, nullable=True)
    favorite_count = Column(Integer, nullable=True)
    num_comments = Column(Integer, nullable=True)
    subreddit = Column(String, nullable=True)
    selftext = Column(String, nullable=True)
    
    # Comment-specific fields
    parent_id = Column(String, nullable=True)  # For comments, links to parent post/comment
    depth = Column(Integer, nullable=True)     # Nesting level for comments
    
    # Relationship with stocks
    stocks = relationship("Stock", secondary=message_stocks, back_populates="messages")

class Stock(Base):
    __tablename__ = "stocks"

    symbol = Column(String, primary_key=True)
    name = Column(String)
    sector = Column(String, nullable=True)
    industry = Column(String, nullable=True)
    last_updated = Column(DateTime, default=datetime.utcnow)
    
    # Relationship with messages
    messages = relationship("DBMessage", secondary=message_stocks, back_populates="stocks") 