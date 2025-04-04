import os
import sys
import logging
import asyncio
from dotenv import load_dotenv
from firebase_admin import initialize_app, credentials, _apps
from firebase_functions import scheduler_fn, options
import google.cloud.logging
from google.cloud import firestore
from bigquery_ops import scrape_reddit_to_bigquery

# Load environment variables first
load_dotenv()

# Initialize Firebase Admin
if not _apps:
    cred = credentials.ApplicationDefault()  # Uses GCP's default credentials
    app = initialize_app(cred)
    
# Get project ID from environment and export it
PROJECT_ID = os.getenv('PROJECT_ID')
if not PROJECT_ID:
    raise ValueError("PROJECT_ID environment variable is not set")
STOCK_DATA_COLLECTION = os.getenv('FIRESTORE_STOCK_DATA_COLLECTION', 'stock_data')
if not STOCK_DATA_COLLECTION:
    raise ValueError("STOCK_DATA_COLLECTION environment variable is not set")
# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Setup Google Cloud Logging only in production
if not os.getenv('FUNCTIONS_EMULATOR'):  # Skip in local development
    try:
        client = google.cloud.logging.Client(project=PROJECT_ID)
        client.setup_logging()
        logger.info("Google Cloud Logging initialized")
    except Exception as e:
        logger.warning(f"Failed to initialize Google Cloud Logging: {e}. Using default logging.")
        # Setup basic logging for local development
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)


@scheduler_fn.on_schedule(
    schedule="0 */2 * * *",  # Run every 2 hours
    memory=options.MemoryOption.GB_1,
    max_instances=3,
    timeout_sec=60*40  # 40 minutes
)
def run_scraper_scheduler(event: scheduler_fn.ScheduledEvent) -> str:
    """Scheduled function that runs every hour to scrape Reddit data"""
    logger.info("Starting scheduled Reddit scraper")
    
    try:
        # Use a sync function to run the async code
        def run_async():
            # Set up a new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # Create async client
                db = firestore.AsyncClient(project=PROJECT_ID)
                
                # Set a longer timeout for the scraper to handle potential network issues
                # Default is None which means no timeout
                timeout = 60 * 40  # 30 minutes timeout
                
                # Run the scraper with timeout
                try:
                    # Create a task for the scraper
                    scraper_task = asyncio.ensure_future(scrape_reddit_to_bigquery(limit=10000))
                    
                    # Run the task with a timeout
                    result = loop.run_until_complete(asyncio.wait_for(scraper_task, timeout))
                    logger.info(f"Scraper completed successfully with {result} messages")
                    return result
                except asyncio.TimeoutError:
                    logger.error(f"Scraper timed out after {timeout} seconds")
                    return 0
                except Exception as e:
                    logger.error(f"Error running scraper: {str(e)}", exc_info=True)
                    
                    # Check for specific network-related errors
                    error_str = str(e)
                    if "Connection reset by peer" in error_str:
                        logger.error("Network error: Connection reset by peer. Reddit may be rate limiting requests.")
                    elif "Timeout" in error_str:
                        logger.error("Network error: Request timeout. Reddit API may be slow or unresponsive.")
                    elif "Too Many Requests" in error_str or "429" in error_str:
                        logger.error("Rate limiting error: Reddit is rate limiting our requests.")
                    
                    return 0
            finally:
                loop.close()
        
        # Run the async code in a synchronous context
        result = run_async()
        logger.info(f"Scheduled scraping completed. Stored {result} messages.")
        return f"Stored {result} messages"
        
    except Exception as e:
        error_msg = f"Scheduled scraping error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return error_msg
