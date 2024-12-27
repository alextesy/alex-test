from abc import ABC
from typing import List
from models.message import Message

class SocialMediaScraper(ABC):
    """Base class for social media scrapers"""
    
    def get_posts(self, query: str, limit: int = 100) -> List[Message]:
        """
        Get posts matching a query.
        Should be implemented by child classes.
        
        Args:
            query (str): Search query
            limit (int): Maximum number of posts to return
            
        Returns:
            List[Message]: List of matching posts
        """
        raise NotImplementedError 