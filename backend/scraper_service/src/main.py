import logging
from datetime import datetime
from time import sleep
from db.database import SessionLocal, init_db
from models.database_models import RawMessage, MessageType
from models.message import Message, Tweet, RedditPost, RedditComment
from scraper_service.src.scrapers.reddit_scraper import RedditScraper
from scraper_service.src.scrapers.twitter_scraper import TwitterScraper

# Set up logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def convert_to_raw_message(message: Message) -> RawMessage:
    """Convert any message type to a RawMessage"""
    base_args = {
        'id': message.id,
        'content': message.content,
        'author': message.author,
        'timestamp': message.timestamp,
        'url': message.url,
        'score': message.score,
        'created_at': message.created_at
    }
    
    # Add type-specific fields based on message type
    if isinstance(message, Tweet):
        return RawMessage(
            **base_args,
            message_type=MessageType.TWEET,
            retweet_count=message.retweet_count,
            favorite_count=message.favorite_count
        )
    elif isinstance(message, RedditComment):
        return RawMessage(
            **base_args,
            message_type=MessageType.REDDIT_COMMENT,
            title=message.title,
            selftext=getattr(message, 'selftext', ''),
            num_comments=message.num_comments,
            subreddit=message.subreddit,
            parent_id=message.parent_id,
            depth=message.depth
        )
    elif isinstance(message, RedditPost):
        return RawMessage(
            **base_args,
            message_type=MessageType.REDDIT_POST,
            title=message.title,
            selftext=message.selftext,
            num_comments=message.num_comments,
            subreddit=message.subreddit
        )
    else:
        raise ValueError(f"Unsupported message type: {type(message)}")

def scrape_reddit(db):
    """Scrape Reddit posts and comments"""
    logger.info("Starting Reddit scraping process")
    reddit_scraper = RedditScraper()
    reddit_posts = []
    
    try:
        # Try to get today's daily discussion thread
        logger.info("Fetching daily discussion thread and comments")
        daily_post, daily_comments = reddit_scraper.get_daily_discussion_comments(limit=None)
        
        # Check if this is a new daily discussion thread
        try:
            with open("last_daily_discussion_id.txt", "r") as f:
                last_discussion_id = f.read().strip()
            is_new_discussion = daily_post.id != last_discussion_id
        except FileNotFoundError:
            is_new_discussion = True
            last_discussion_id = None
        
        if is_new_discussion:
            logger.info(f"Found new daily discussion thread: {daily_post.title}")
            # Store the new daily discussion ID
            with open("last_daily_discussion_id.txt", "w") as f:
                f.write(daily_post.id)
            reddit_posts.extend(daily_comments)
            logger.info(f"Retrieved {len(daily_comments)} comments from new daily discussion")
        else:
            logger.info("Found existing daily discussion thread, checking for new comments")
            new_comments = reddit_scraper.get_new_comments(daily_post.id)
            reddit_posts.extend(new_comments)
            logger.info(f"Retrieved {len(new_comments)} new comments")
    
        logger.info("Processing and storing Reddit messages")
        for post in reddit_posts:
            try:
                raw_msg = convert_to_raw_message(post)
                db.add(raw_msg)
                logger.debug(f"Stored Reddit message {post.id}")
            except Exception as e:
                logger.error(f"Failed to store Reddit message {post.id}: {str(e)}")
                continue
    
        return len(reddit_posts)
    except Exception as e:
        logger.error(f"Error in Reddit scraping process: {str(e)}")
        raise

def scrape_twitter(db):
    """Scrape Twitter posts"""
    logger.info("Starting Twitter scraping process")
    twitter_scraper = TwitterScraper()
    tweets = []
    
    try:
        # General stock tweets
        logger.info("Fetching general stock-related tweets")
        general_tweets = twitter_scraper.get_posts(query="stocks OR investing", limit=50)
        tweets.extend(general_tweets)
        logger.info(f"Retrieved {len(general_tweets)} general stock tweets")
        
        # Stock-specific tweets
        stock_symbols = ["SPY", "QQQ", "AAPL", "MSFT", "TSLA"]
        logger.info(f"Fetching tweets for specific symbols: {', '.join(stock_symbols)}")
        
        for symbol in stock_symbols:
            logger.info(f"Searching tweets for symbol: {symbol}")
            symbol_tweets = twitter_scraper.search_stock_mentions(symbol, time_filter='day')
            tweets.extend(symbol_tweets)
            logger.info(f"Retrieved {len(symbol_tweets)} tweets for {symbol}")
        
        logger.info("Processing and storing Twitter messages")
        for tweet in tweets:
            try:
                raw_msg = convert_to_raw_message(tweet)
                db.add(raw_msg)
                logger.debug(f"Stored Twitter message {tweet.id}")
            except Exception as e:
                logger.error(f"Failed to store Twitter message {tweet.id}: {str(e)}")
                continue
        
        return len(tweets)
    except Exception as e:
        logger.error(f"Error in Twitter scraping process: {str(e)}")
        raise

def run_scraper(enable_twitter=False):
    """Main scraping loop"""
    logger.info("Initializing scraper service")
    cycle_count = 0
    
    while True:
        cycle_count += 1
        logger.info(f"Starting scraping cycle #{cycle_count}")
        cycle_start_time = datetime.now()
        
        try:
            logger.info("Creating database session")
            db = SessionLocal()
            
            # Scrape Reddit
            logger.info("Starting Reddit scraping")
            reddit_count = scrape_reddit(db)
            logger.info(f"Successfully scraped {reddit_count} Reddit posts/comments")
            
            # Optionally scrape Twitter
            if enable_twitter:
                logger.info("Starting Twitter scraping")
                twitter_count = scrape_twitter(db)
                logger.info(f"Successfully scraped {twitter_count} tweets")
            
            db.commit()
            cycle_duration = datetime.now() - cycle_start_time
            logger.info(f"Scraping cycle #{cycle_count} completed in {cycle_duration}")
            
        except Exception as e:
            logger.error(f"Error during scraping cycle #{cycle_count}: {str(e)}", exc_info=True)
            db.rollback()
            logger.info("Database changes rolled back due to error")
        
        finally:
            db.close()
            logger.info("Database session closed")
            
        # Wait before next scraping cycle
        logger.info("Waiting 5 minutes before next scraping cycle")
        sleep(300)  # 5 minutes

if __name__ == "__main__":
    logger.info("Starting scraper service...")
    
    # Initialize database tables
    logger.info("Initializing database...")
    init_db()
    
    # Start the scraper
    run_scraper() 