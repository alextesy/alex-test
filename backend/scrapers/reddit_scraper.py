import praw
import os
from dotenv import load_dotenv
from typing import List
from .base_scraper import SocialMediaScraper
from ..models.message import RedditPost, Message

load_dotenv()

class RedditScraper(SocialMediaScraper):
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT', 'StockSentimentBot 1.0')
        )

    def get_posts(self, query: str, limit: int = 100) -> List[Message]:
        """
        Fetch posts from a specified subreddit.
        """
        subreddit = self.reddit.subreddit(query)
        posts = []

        try:
            for post in subreddit.hot(limit=limit):
                post_obj = RedditPost(
                    id=post.id,
                    content=post.selftext,
                    author=str(post.author) if post.author else '[deleted]',
                    timestamp=post.created_utc,
                    url=post.url,
                    score=post.score,
                    platform='reddit',
                    source=f"reddit/r/{query}",
                    title=post.title,
                    selftext=post.selftext,
                    num_comments=post.num_comments,
                    subreddit=query
                )
                posts.append(post_obj)
                
        except Exception as e:
            print(f"Error fetching posts from r/{query}: {str(e)}")
            
        return posts

    def search_stock_mentions(self, stock_symbol: str, subreddits: List[str] = None) -> List[Message]:
        """
        Search for mentions of a specific stock across specified subreddits.
        """
        if subreddits is None:
            subreddits = ['wallstreetbets', 'stocks', 'investing']

        all_mentions = []
        
        for subreddit_name in subreddits:
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                search_query = f'"{stock_symbol}"'
                
                for post in subreddit.search(search_query, limit=50):
                    post_obj = RedditPost(
                        id=post.id,
                        content=post.selftext,
                        author=str(post.author) if post.author else '[deleted]',
                        timestamp=post.created_utc,
                        url=post.url,
                        score=post.score,
                        platform='reddit',
                        source=f"reddit/r/{subreddit_name}",
                        title=post.title,
                        selftext=post.selftext,
                        num_comments=post.num_comments,
                        subreddit=subreddit_name
                    )
                    all_mentions.append(post_obj)
                    
            except Exception as e:
                print(f"Error searching in r/{subreddit_name}: {str(e)}")
                
        return all_mentions