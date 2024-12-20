from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Set
from ..processors.text_processor import TextProcessor

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
    mentioned_stocks: Set[str] = field(default_factory=set)
    sentiment: float = 0.0
    
    # Class-level processor instance
    _processor: Optional[TextProcessor] = None
    
    @classmethod
    def get_processor(cls) -> TextProcessor:
        """Get or create the text processor instance"""
        if cls._processor is None:
            cls._processor = TextProcessor()
        return cls._processor
    
    def __post_init__(self):
        """Process text after initialization"""
        self.process_text()
    
    def process_text(self) -> None:
        """Extract stock mentions and sentiment from text"""
        processor = self.get_processor()
        title = getattr(self, 'title', '')
        
        # Process text and update attributes
        self.mentioned_stocks, self.sentiment = processor.analyze_text(
            text=self.content,
            title=title
        )

@dataclass
class Tweet(Message):
    """Twitter message model"""
    retweet_count: int
    favorite_count: int
    title: str = ''  # Twitter doesn't have titles
    
    @property
    def comments_count(self) -> int:
        """Alias for retweet_count to maintain consistency with other platforms"""
        return self.retweet_count

@dataclass
class RedditPost(Message):
    """Reddit post model"""
    title: str
    selftext: str
    num_comments: int
    subreddit: str
    
    @property
    def comments_count(self) -> int:
        """Alias for num_comments to maintain consistency with other platforms"""
        return self.num_comments