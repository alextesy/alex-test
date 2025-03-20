import os
import logging
from firebase_admin import initialize_app, credentials, _apps
from dotenv import load_dotenv
import google.cloud.logging
import sys
from datetime import datetime, timedelta

def convert_timestamp(timestamp) -> datetime:
    """Convert various timestamp formats to datetime object."""
    if isinstance(timestamp, datetime):
        return timestamp
    elif isinstance(timestamp, (int, float)):
        return datetime.fromtimestamp(timestamp)
    elif timestamp is None:
        return None
    else:
        try:
            return datetime.fromisoformat(str(timestamp))
        except (ValueError, TypeError):
            return None

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

# Export the scheduled functions
from firebase_functions import scheduler_fn, options
import asyncio
from google.cloud import firestore
from google.cloud import bigquery
from firestore_ops import scrape_reddit

@scheduler_fn.on_schedule(
    schedule="0 */6 * * *",  # Run every 6 hours
    memory=options.MemoryOption.GB_1,
    max_instances=3,
    timeout_sec=1800  # 30 minutes
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
                timeout = 1200  # 20 minutes timeout
                
                # Run the scraper with timeout
                try:
                    # Create a task for the scraper
                    scraper_task = asyncio.ensure_future(scrape_reddit(db))
                    
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

# Get collection name from environment
STOCK_DATA_COLLECTION = os.getenv('FIRESTORE_STOCK_DATA_COLLECTION', 'stock_data')

def delete_firestore_docs(db: firestore.Client, doc_refs: list, batch_size: int = 500) -> int:
    """Delete documents from Firestore in batches.
    
    Args:
        db: Firestore client
        doc_refs: List of document references to delete
        batch_size: Size of each deletion batch (max 500)
        
    Returns:
        int: Number of documents deleted
    """
    total_deleted = 0
    for i in range(0, len(doc_refs), batch_size):
        batch = db.batch()
        batch_docs = doc_refs[i:i + batch_size]
        
        for doc_ref in batch_docs:
            batch.delete(doc_ref)
            
        batch.commit()
        total_deleted += len(batch_docs)
        logger.info(f"Deleted {len(batch_docs)} documents from Firestore")
    
    return total_deleted

def transform_firestore_doc(doc: firestore.DocumentSnapshot) -> dict:
    """Transform a Firestore document into BigQuery row format.
    
    Args:
        doc: Firestore document snapshot
        
    Returns:
        dict: Transformed row ready for BigQuery insertion
    """
    data = doc.to_dict()
    return {
        'document_id': doc.id,
        'message_id': data.get('id'),
        'content': data.get('content'),
        'author': data.get('author'),
        'timestamp': convert_timestamp(data.get('timestamp')).isoformat() if data.get('timestamp') else None,
        'url': data.get('url'),
        'score': data.get('score'),
        'created_at': convert_timestamp(data.get('created_at')).isoformat() if data.get('created_at') else None,
        'message_type': data.get('message_type'),
        'source': data.get('source'),
        'title': data.get('title'),
        'selftext': data.get('selftext'),
        'num_comments': data.get('num_comments'),
        'subreddit': data.get('subreddit'),
        'parent_id': data.get('parent_id'),
        'depth': data.get('depth'),
        'ingestion_timestamp': datetime.utcnow().isoformat()
    }

def process_chunk(bq_client: bigquery.Client, db: firestore.Client, chunk_docs: list, table_id: str, chunk_number: int, total_chunks: int) -> tuple[int, int]:
    """Process a chunk of documents - transform, insert to BigQuery, and delete from Firestore.
    
    Args:
        bq_client: BigQuery client
        db: Firestore client
        chunk_docs: List of documents to process
        table_id: BigQuery table ID
        chunk_number: Current chunk number (1-based)
        total_chunks: Total number of chunks
        
    Returns:
        tuple[int, int]: Number of rows inserted and documents deleted
    """
    chunk_doc_refs = []
    rows_to_insert = []
    
    logger.info(f"Processing chunk {chunk_number} of {total_chunks}")
    
    # Transform documents
    for doc in chunk_docs:
        chunk_doc_refs.append(doc.reference)
        rows_to_insert.append(transform_firestore_doc(doc))
    
    logger.info(f"Inserting {len(rows_to_insert)} rows for chunk {chunk_number}")
    
    # Insert to BigQuery
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        error_msg = f'Errors inserting rows in chunk {chunk_number}: {errors}'
        logger.error(error_msg)
        raise Exception(error_msg)
    
    logger.info(f"Successfully inserted {len(rows_to_insert)} rows")
    
    # Delete from Firestore
    docs_deleted = delete_firestore_docs(db, chunk_doc_refs)
    
    logger.info(f"Completed chunk {chunk_number}. Inserted: {len(rows_to_insert)}, Deleted: {docs_deleted}")
    return len(rows_to_insert), docs_deleted

@scheduler_fn.on_schedule(
    schedule="0 */6 * * *",  # Run every 6 hours
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