from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Set

@dataclass
class Message(ABC):
    """Base class for all social media messages"""
    id: str
    content: str
    author: str
    timestamp: float
    url: str
    score: int
    created_at: datetime = field(default_factory=datetime.utcnow)
    sentiment: float = 0.0
    message_type: str = field(init=False)  # Will be set by child classes
    
    # Simplified base class without database conversion methods

@dataclass
class RedditPost(Message):
    """Reddit post model"""
    title: str = ''
    selftext: str = ''
    num_comments: int = 0
    subreddit: str = ''
    submission_id: str = ''

    def __post_init__(self):
        self.message_type = "REDDIT_POST"
    
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
        self.message_type = "REDDIT_COMMENT"
    
    @property
    def comments_count(self) -> int:
        """Number of replies to this comment"""
        return self.num_comments

@dataclass
class CNBCArticle(Message):
    """CNBC article model"""
    title: str = ''  # Make title optional with default empty string
    summary: str = ''
    category: str = '' 
    author_title: str = ''  # Author's title/position at CNBC
    
    def __post_init__(self):
        self.message_type = "CNBC_ARTICLE"
    
    @property
    def comments_count(self) -> int:
        """CNBC articles don't have comments in our system"""
        return 0