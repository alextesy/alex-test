import tweepy
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

class TwitterScraper:
    def __init__(self):
        self.auth = tweepy.OAuth2AppHandler(
            os.getenv('X_API_KEY'),
            os.getenv('X_API_SECRET')
        )
        self.api = tweepy.API(self.auth)

    def get_tweets_for_stock(self, symbol, days=1):
        try:
            # Search for tweets containing the stock symbol
            query = f"${symbol} -filter:retweets"
            tweets = self.api.search_tweets(
                q=query,
                lang="en",
                count=1,
                tweet_mode="extended"
            )
            
            return [{
                'text': tweet.full_text,
                'created_at': tweet.created_at,
                'user': tweet.user.screen_name,
                'likes': tweet.favorite_count,
                'retweets': tweet.retweet_count
            } for tweet in tweets]
        
        except Exception as e:
            print(f"Error fetching tweets: {str(e)}")
            return [] 