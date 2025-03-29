import os
import logging
import asyncio
import json
from dotenv import load_dotenv
from firebase_admin import initialize_app, credentials, _apps
from firebase_functions import https_fn
from google.cloud import logging as cloud_logging
from scrapers.reddit_scraper_v2 import RedditScraper

# Load environment variables
load_dotenv()

# Initialize Firebase Admin
if not _apps:
    cred = credentials.ApplicationDefault()  # Uses GCP's default credentials
    app = initialize_app(cred)
    
# Get project ID from environment
PROJECT_ID = os.getenv('PROJECT_ID')
if not PROJECT_ID:
    raise ValueError("PROJECT_ID environment variable is not set")

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Setup Google Cloud Logging only in production
if not os.getenv('FUNCTIONS_EMULATOR'):  # Skip in local development
    try:
        client = cloud_logging.Client(project=PROJECT_ID)
        client.setup_logging()
        logger.info("Google Cloud Logging initialized")
    except Exception as e:
        logger.warning(f"Failed to initialize Google Cloud Logging: {e}. Using default logging.")
        # Setup basic logging for local development
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)

@https_fn.on_request(memory=https_fn.options.MemoryOption.GB_1, timeout_sec=540)
async def scrape_reddit(req: https_fn.Request) -> https_fn.Response:
    """Cloud Function that scrapes Reddit data.
    
    Args:
        req: The request object
        
    Returns:
        The response object with the scraping results
    """
    logger.info("Starting Reddit scraper function")
    
    try:
        # Parse request to get limit parameter
        request_json = req.get_json() if req.is_json else {}
        limit = request_json.get('limit', 10000)
        
        # Use async function to handle asynchronous scraping
        count = await scrape_reddit_async(limit)
            
        # Return response
        return https_fn.Response(
            json.dumps({"status": "success", "count": count}),
            status=200,
            headers={"Content-Type": "application/json"}
        )
        
    except Exception as e:
        error_msg = f"Error in Reddit scraper function: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return https_fn.Response(
            json.dumps({"status": "error", "message": error_msg}),
            status=500,
            headers={"Content-Type": "application/json"}
        )

async def scrape_reddit_async(limit: int = 10000) -> int:
    """Performs the actual Reddit scraping.
    
    Args:
        limit: Maximum number of posts/comments to scrape
        
    Returns:
        int: Number of messages scraped
    """
    logger.info(f"Starting Reddit scraping with limit {limit}")
    message_count = 0
    
    try:
        # Create and use the scraper as a context manager
        async with RedditScraper() as scraper:
            # Fetch posts from all subreddits
            posts = await scraper.fetch_posts_from_all_subreddits(limit=min(100, limit), sort="hot")
            message_count += len(posts)
            logger.info(f"Fetched {len(posts)} posts from all subreddits")
            
            # Fetch comments for each post (up to the limit)
            remaining_limit = limit - message_count
            for post in posts:
                if remaining_limit <= 0:
                    break
                    
                try:
                    _, comments = await scraper.fetch_post_with_comments(
                        post.id, 
                        comment_limit=min(remaining_limit, 1000)
                    )
                    message_count += len(comments)
                    remaining_limit -= len(comments)
                    logger.info(f"Fetched {len(comments)} comments for post {post.id}")
                except Exception as post_error:
                    logger.error(f"Error fetching comments for post {post.id}: {str(post_error)}")
                    continue
            
            # Also check any daily discussion threads
            if remaining_limit > 0:
                try:
                    post, comments = await scraper.fetch_daily_discussion(limit=remaining_limit)
                    if post:
                        message_count += 1  # Count the discussion post itself
                    if comments:
                        message_count += len(comments)
                        logger.info(f"Fetched {len(comments)} comments from daily discussion")
                except Exception as e:
                    logger.error(f"Error fetching daily discussion: {str(e)}")
        
        logger.info(f"Completed Reddit scraping. Total messages: {message_count}")
        return message_count
        
    except Exception as e:
        logger.error(f"Error in scrape_reddit_async: {str(e)}", exc_info=True)
        raise 