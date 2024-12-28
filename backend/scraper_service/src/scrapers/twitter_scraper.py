import logging
import os
import tweepy
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Optional
from backend.models.message import Tweet, Message
from .base_scraper import SocialMediaScraper

load_dotenv()

logger = logging.getLogger(__name__)

class TwitterScraper(SocialMediaScraper):
    def __init__(self):
        logger.info("Initializing Twitter scraper")
        try:
            auth = tweepy.OAuthHandler(
                os.getenv('X_API_KEY'),
                os.getenv('X_API_SECRET')
            )
            self.api = tweepy.API(auth)
            logger.info("Successfully initialized Twitter API client")
        except Exception as e:
            logger.error(f"Failed to initialize Twitter API client: {str(e)}")
            raise

    def get_posts(self, query: str, limit: int = 100, time_filter: Optional[str] = None) -> List[Message]:
        """
        Fetch tweets based on a search query.
        
        Args:
            query (str): Search query string
            limit (int): Maximum number of tweets to return (default: 100)
            time_filter (str, optional): Time range for tweets ('day', 'week', 'month')
        """
        logger.info(f"Searching tweets with query: '{query}', limit: {limit}")
        tweets = []
        try:
            # Convert time_filter to Twitter's expected format
            original_query = query
            if time_filter:
                if time_filter == 'day':
                    query += " since:24h"
                elif time_filter == 'week':
                    query += " since:7d"
                elif time_filter == 'month':
                    query += " since:30d"
                logger.debug(f"Modified query with time filter: '{query}'")
                    
            logger.info("Starting tweet collection")
            tweet_count = 0
            for tweet in tweepy.Cursor(self.api.search_tweets, q=query, tweet_mode="extended").items(limit):
                try:
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
                    tweet_count += 1
                    logger.debug(f"Processed tweet {tweet.id_str} from @{tweet.user.screen_name}")
                except Exception as e:
                    logger.error(f"Error processing tweet {tweet.id_str}: {str(e)}")
                    continue
                    
            logger.info(f"Retrieved {tweet_count} tweets for query '{original_query}'")
            
        except Exception as e:
            logger.error(f"Error fetching tweets for query '{query}': {str(e)}")
            
        return tweets

    def search_stock_mentions(self, stock_symbol: str, time_filter: Optional[str] = None, include_cashtag: bool = True) -> List[Message]:
        """
        Search for mentions of a specific stock on Twitter.
        
        Args:
            stock_symbol (str): Stock symbol to search for
            time_filter (str, optional): Time range for tweets ('day', 'week', 'month')
            include_cashtag (bool): Whether to include cashtag search (default: True)
        """
        logger.info(f"Searching for mentions of stock {stock_symbol}")
        search_queries = [stock_symbol]
        if include_cashtag:
            search_queries.append(f"${stock_symbol}")
            logger.debug(f"Including cashtag search with queries: {search_queries}")
            
        all_mentions = []
        for query in search_queries:
            logger.info(f"Searching with query: '{query}'")
            mentions = self.get_posts(query, limit=50, time_filter=time_filter)
            all_mentions.extend(mentions)
            logger.debug(f"Found {len(mentions)} mentions for query '{query}'")
            
        logger.info(f"Total mentions found for {stock_symbol}: {len(all_mentions)}")
        return all_mentions 