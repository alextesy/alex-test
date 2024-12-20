from abc import ABC, abstractmethod
from typing import List
from ..models.message import Message

class SocialMediaScraper(ABC):
    """Abstract base class for social media scrapers"""
    
    @abstractmethod
    def get_posts(self, query: str, limit: int = 100) -> List[Message]:
        """
        Get posts from the social media platform.
        
        Args:
            query (str): Search query or identifier (e.g., subreddit name, hashtag)
            limit (int): Maximum number of posts to fetch
            
        Returns:
            List[Message]: List of posts with standardized information
        """
        pass

    @abstractmethod
    def search_stock_mentions(self, stock_symbol: str, **kwargs) -> List[Message]:
        """
        Search for mentions of a specific stock.
        
        Args:
            stock_symbol (str): Stock symbol to search for (e.g., 'AAPL')
            **kwargs: Platform-specific search parameters
            
        Returns:
            List[Message]: List of posts mentioning the stock symbol
        """
        pass 