import tweepy
import os
from dotenv import load_dotenv
from typing import List
from .base_scraper import SocialMediaScraper
from ..models.message import Tweet, Message

load_dotenv()

class TwitterScraper(SocialMediaScraper):
    def __init__(self):
        auth = tweepy.OAuthHandler(
            os.getenv('X_API_KEY'),
            os.getenv('X_API_SECRET')
        )
        self.api = tweepy.API(auth)

    def get_posts(self, query: str, limit: int = 100) -> List[Message]:
        """
        Fetch tweets based on a search query.
        """
        tweets = []
        try:
            for tweet in tweepy.Cursor(self.api.search_tweets, q=query, tweet_mode="extended").items(limit):
                tweet_obj = Tweet(
                    id=tweet.id_str,
                    content=tweet.full_text,
                    author=tweet.user.screen_name,
                    timestamp=tweet.created_at.timestamp(),
                    url=f"https://twitter.com/user/status/{tweet.id_str}",
                    score=tweet.favorite_count,
                    platform='twitter',
                    source='twitter',
                    retweet_count=tweet.retweet_count,
                    favorite_count=tweet.favorite_count
                )
                tweets.append(tweet_obj)
        except Exception as e:
            print(f"Error fetching tweets for query '{query}': {str(e)}")
            
        return tweets

    def search_stock_mentions(self, stock_symbol: str, include_cashtag: bool = True) -> List[Message]:
        """
        Search for mentions of a specific stock on Twitter.
        """
        search_queries = [stock_symbol]
        if include_cashtag:
            search_queries.append(f"${stock_symbol}")
            
        all_mentions = []
        for query in search_queries:
            mentions = self.get_posts(query, limit=50)
            all_mentions.extend(mentions)
            
        return all_mentions 