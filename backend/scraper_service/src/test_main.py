import sys
import os
from datetime import datetime
import logging

# Add the backend directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from db.database import SessionLocal, init_db
from scraper_service.src.main import scrape_reddit
from scraper_service.src.scrapers.reddit_scraper import RedditScraper

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_scraper_small_sample():
    """
    Integration test that runs the actual scraper with a small data sample.
    This will actually connect to Reddit but limit the amount of data fetched.
    """
    logger.info("Starting small sample scraper test")
    
    # Initialize database
    init_db()
    db = SessionLocal()
    
    try:
        # Override the RedditScraper's get_daily_discussion_comments to limit comments
        original_method = RedditScraper.get_daily_discussion_comments
        def limited_get_daily_discussion(self, limit=None):
            # Force a small limit
            return original_method(self, limit=3)
        
        RedditScraper.get_daily_discussion_comments = limited_get_daily_discussion
        
        # Run the scraper
        logger.info("Running scraper with limited comments")
        num_messages = scrape_reddit(db)
        
        logger.info(f"Scraper completed. Processed {num_messages} messages")
        
        # Commit changes
        db.commit()
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        db.rollback()
        raise
    finally:
        # Restore original method
        RedditScraper.get_daily_discussion_comments = original_method
        db.close()
        logger.info("Test completed")

if __name__ == "__main__":
    test_scraper_small_sample() 