import logging
import firebase_functions
from firebase_functions import https_fn, scheduler_fn, options
import os
from firebase_admin import initialize_app, firestore
from datetime import datetime
from time import sleep
from models.message import Message, RedditPost, RedditComment
from scrapers.reddit_scraper import RedditScraper
# Commenting out Twitter imports
# from scrapers.twitter_scraper import TwitterScraper

# Initialize Firebase Admin
initialize_app()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Firestore collection names
STOCK_DATA_COLLECTION = os.getenv('FIRESTORE_STOCK_DATA_COLLECTION')
SCRAPER_STATE_COLLECTION = os.getenv('FIRESTORE_SCRAPER_STATE_COLLECTION')

async def store_message_in_firestore(message: Message, db_firestore):
    """Store a message in Firestore"""
    try:
        # Convert message to dict for Firestore storage
        base_data = {
            'id': message.id,
            'content': message.content,
            'author': message.author,
            'timestamp': message.timestamp,
            'url': message.url,
            'score': message.score,
            'created_at': message.created_at,
            'message_type': message.message_type,
            'source': 'reddit'
        }
        
        # Twitter handling commented out
        # if isinstance(message, Tweet):
        #     base_data.update({
        #         'retweet_count': message.retweet_count,
        #         'favorite_count': message.favorite_count
        #     })
        if isinstance(message, (RedditPost, RedditComment)):
            base_data.update({
                'title': message.title,
                'selftext': getattr(message, 'selftext', ''),
                'num_comments': message.num_comments,
                'subreddit': message.subreddit
            })
            if isinstance(message, RedditComment):
                base_data.update({
                    'parent_id': message.parent_id,
                    'depth': message.depth
                })
        
        # Store in Firestore using configured collection name
        doc_ref = db_firestore.collection(STOCK_DATA_COLLECTION).document(message.id)
        await doc_ref.set(base_data)
        logger.debug(f"Stored message {message.id} in Firestore collection {STOCK_DATA_COLLECTION}")
        return True
    except Exception as e:
        logger.error(f"Failed to store message {message.id} in Firestore: {str(e)}")
        return False

async def scrape_reddit(db_firestore, limit=None):
    """Scrape Reddit posts and comments"""
    logger.info("Starting Reddit scraping process")
    reddit_scraper = RedditScraper()
    reddit_posts = []
    
    try:
        state_ref = db_firestore.collection(SCRAPER_STATE_COLLECTION).document('reddit')
        state_doc = await state_ref.get()
        
        # Get last discussion ID from Firestore
        last_discussion_id = state_doc.get('last_daily_discussion_id') if state_doc.exists else None
        logger.info(f"Found last discussion ID: {last_discussion_id}" if last_discussion_id else "No previous daily discussion ID found")

        # Try to get today's daily discussion thread with retries
        logger.info("Fetching daily discussion thread and comments")
        max_retries = 3
        retry_delay = 60  # seconds
        daily_post = None
        daily_comments = []
        
        for attempt in range(max_retries):
            try:
                daily_post, daily_comments = reddit_scraper.get_daily_discussion_comments(
                    limit=limit, 
                    last_discussion_id=last_discussion_id
                )
                if daily_post is not None:
                    break
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed to get daily discussion, retrying in {retry_delay} seconds")
                sleep(retry_delay)
            except Exception as e:
                logger.error(f"Error on attempt {attempt + 1}/{max_retries}: {str(e)}")
                if attempt < max_retries - 1:
                    sleep(retry_delay)
                    continue
                raise
        
        if daily_post is None:
            logger.error("Failed to fetch daily discussion thread after all retries")
            return 0
        
        # If we got here, we have a new daily discussion thread
        logger.info(f"Found new daily discussion thread: {daily_post.title}")
        
        # Store the new daily discussion ID in Firestore
        await state_ref.set({
            'last_daily_discussion_id': daily_post.id,
            'last_updated': datetime.now()
        })
        
        reddit_posts.extend(daily_comments)
        logger.info(f"Retrieved {len(daily_comments)} comments from new daily discussion")
    
        logger.info("Processing and storing Reddit messages")
        stored_count = 0
        for post in reddit_posts:
            try:
                if await store_message_in_firestore(post, db_firestore):
                    stored_count += 1
                    logger.debug(f"Stored Reddit message {post.id}")
            except Exception as e:
                logger.error(f"Failed to store Reddit message {post.id}: {str(e)}")
                continue
                
        return stored_count
                
    except Exception as e:
        logger.error(f"Error in Reddit scraping process: {str(e)}")
        return 0

# Set max instances to 3
@scheduler_fn.on_schedule(
    schedule="0 * * * *",  # Run every hour
    memory=options.MemoryOption.GB_1,
    max_instances=3
)
@https_fn.on_request(
    memory=options.MemoryOption.GB_1,
    max_instances=3
)
async def run_scraper(request: https_fn.Request | scheduler_fn.ScheduledEvent) -> https_fn.Response:
    """Firebase Cloud Function to run the scraper. Can be triggered via HTTP or schedule."""
    logger.info("Initializing scraper service")
    
    try:
        db_firestore = firestore.client()
        
        # Scrape Reddit
        logger.info("Starting Reddit scraping")
        reddit_count = await scrape_reddit(db_firestore)
        logger.info(f"Successfully scraped {reddit_count} Reddit posts/comments")
        
        # Twitter scraping disabled for now
        # Check if Twitter scraping is enabled via query parameter
        # enable_twitter = request.args.get('enable_twitter', '').lower() == 'true'
        # twitter_count = 0
        # if enable_twitter:
        #     logger.info("Starting Twitter scraping")
        #     twitter_count = await scrape_twitter(db_firestore)
        #     logger.info(f"Successfully scraped {twitter_count} tweets")
        
        return https_fn.Response(
            json={'status': 'success', 'reddit_count': reddit_count},
            status=200
        )
    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}", exc_info=True)
        return https_fn.Response(
            json={'status': 'error', 'message': str(e)},
            status=500
        ) 