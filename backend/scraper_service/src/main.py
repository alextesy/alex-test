import logging
from datetime import datetime
from time import sleep
from db.database import SessionLocal, init_db
from models.database_models import RawMessage
from scraper_service.src.scrapers.reddit_scraper import RedditScraper
from scraper_service.src.scrapers.twitter_scraper import TwitterScraper

# Set up logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def scrape_reddit(db):
    """Scrape Reddit posts and comments"""
    logger.info("Starting Reddit scraping process")
    reddit_scraper = RedditScraper()
    reddit_posts = []
    
    try:
        # Get the daily discussion thread and its comments
        logger.info("Fetching daily discussion thread and comments")
        daily_post, daily_comments = reddit_scraper.get_daily_discussion_comments(limit=5)
        
        if daily_post:
            logger.info(f"Found daily discussion thread: {daily_post.title}")
            reddit_posts.extend(daily_comments)
            logger.info(f"Retrieved {len(daily_comments)} comments from daily discussion")
        else:
            logger.warning("No daily discussion thread found")
    
        logger.info("Processing and storing Reddit messages")
        for post in reddit_posts:
            try:
                raw_msg = RawMessage(
                    id=post.id,
                    content=post.content,
                    author=post.author,
                    timestamp=post.timestamp,
                    url=post.url,
                    score=post.score,
                    platform="reddit",
                    title=getattr(post, 'title', None),
                    parent_id=getattr(post, 'parent_id', None)
                )
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
                raw_msg = RawMessage(
                    id=tweet.id,
                    content=tweet.content,
                    author=tweet.author,
                    timestamp=tweet.timestamp,
                    url=tweet.url,
                    score=tweet.score,
                    platform="twitter"
                )
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