import os
import sys
import logging
import asyncio
from dotenv import load_dotenv
from firebase_admin import initialize_app, credentials, _apps
from firebase_functions import scheduler_fn, options
import google.cloud.logging
from google.cloud import firestore, bigquery
from bigquery_ops import process_chunk
from firestore_ops import scrape_reddit

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
                    scraper_task = asyncio.ensure_future(scrape_reddit(db, limit=10000))
                    
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


@scheduler_fn.on_schedule(
    schedule="0 */2 * * *",  # Run every 2 hours
    memory=options.MemoryOption.GB_1,
    max_instances=3,
    timeout_sec=1800  # 30 minutes
)
def firestore_to_bigquery_etl(event: scheduler_fn.ScheduledEvent) -> str:
    """Cloud Function triggered hourly to dump all Firestore stock data to BigQuery and delete from Firestore.
    
    This function performs a complete ETL process:
    1. Extracts all data from Firestore
    2. Transforms it to fit BigQuery schema
    3. Loads it into BigQuery in chunks
    4. Deletes the processed data from Firestore in chunks
    
    Args:
        event (ScheduledEvent): The event that triggered this function.
    Returns:
        str: Status message
    """
    
    try:
        # Initialize clients
        db = firestore.Client()
        bq_client = bigquery.Client()
        
        logger.info("Fetching all data from Firestore collection")
        
        # Query Firestore for all data
        collection_ref = db.collection(STOCK_DATA_COLLECTION)
        docs = list(collection_ref.stream())
        total_docs = len(docs)
        logger.info(f"Found {total_docs} documents to process")
        
        if total_docs == 0:
            logger.info('No data to insert')
            return 'No data to insert'

        # Get project ID from environment
        project_id = os.getenv('PROJECT_ID')
        if not project_id:
            raise ValueError("PROJECT_ID environment variable is not set")
            
        # Define table reference
        dataset_id = os.getenv('BIGQUERY_DATASET_ID', 'reddit_data')
        table_id = f"{project_id}.{dataset_id}.raw_messages"
        
        # Process in chunks
        chunk_size = 500  # Match Firestore's batch limit
        total_inserted = 0
        total_deleted = 0
        total_chunks = (total_docs + chunk_size - 1) // chunk_size
        
        for i in range(0, total_docs, chunk_size):
            chunk_docs = docs[i:i + chunk_size]
            chunk_number = i // chunk_size + 1
            
            # Process the chunk
            inserted, deleted = process_chunk(
                bq_client=bq_client,
                db=db,
                chunk_docs=chunk_docs,
                table_id=table_id,
                chunk_number=chunk_number,
                total_chunks=total_chunks
            )
            
            total_inserted += inserted
            total_deleted += deleted
            
        success_msg = f'Successfully inserted {total_inserted} rows to BigQuery and deleted {total_deleted} documents from Firestore'
        logger.info(success_msg)
        return success_msg
        
    except Exception as e:
        error_msg = f'Error in firestore_to_bigquery_etl: {str(e)}'
        logger.error(error_msg, exc_info=True)
        raise 