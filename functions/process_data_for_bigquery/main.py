import os
import logging
import json
import time
from typing import Tuple
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import logging as cloud_logging
from firebase_admin import initialize_app, credentials, _apps
from firebase_functions import https_fn

# Initialize Firebase Admin
if not _apps:
    cred = credentials.ApplicationDefault()  # Uses GCP's default credentials
    app = initialize_app(cred)

# Get project ID and collection names from environment
PROJECT_ID = os.getenv('PROJECT_ID')
if not PROJECT_ID:
    raise ValueError("PROJECT_ID environment variable is not set")
DATASET_ID = os.getenv('BIGQUERY_DATASET_ID', 'reddit_data')
SCRAPER_STATE_COLLECTION = os.getenv('FIRESTORE_SCRAPER_STATE_COLLECTION', 'scraper_state')

# Define the BigQuery table ID
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.raw_messages"

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
def process_data_for_bigquery(req: https_fn.Request) -> https_fn.Response:
    """Cloud Function to process Reddit data from Firestore and store it in BigQuery.
    
    Args:
        req: The request object
        
    Returns:
        The response object with processing results
    """
    logger.info("Starting data processing for BigQuery")
    
    try:
        # Create clients
        bq_client = bigquery.Client(project=PROJECT_ID)
        db = firestore.Client(project=PROJECT_ID)
        
        # Process data from Firestore to BigQuery
        processed_count = process_firestore_to_bigquery(bq_client, db)
        
        # Return success response
        return https_fn.Response(
            json.dumps({"status": "success", "count": processed_count}),
            status=200,
            headers={"Content-Type": "application/json"}
        )
    except Exception as e:
        error_msg = f"Error processing data for BigQuery: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return https_fn.Response(
            json.dumps({"status": "error", "message": error_msg}),
            status=500,
            headers={"Content-Type": "application/json"}
        )

def process_firestore_to_bigquery(bq_client: bigquery.Client, db: firestore.Client) -> int:
    """Process data from Firestore to BigQuery.
    
    Args:
        bq_client: BigQuery client
        db: Firestore client
        
    Returns:
        int: Number of records processed
    """
    logger.info("Fetching messages from Firestore")
    
    try:
        # Query all reddit messages in Firestore
        messages = db.collection('messages').limit(10000).get()
        message_list = list(messages)
        
        if not message_list:
            logger.info("No messages found in Firestore")
            return 0
            
        logger.info(f"Found {len(message_list)} messages in Firestore")
        
        # Process in chunks of 500 documents
        chunk_size = 500
        total_processed = 0
        chunks = [message_list[i:i + chunk_size] for i in range(0, len(message_list), chunk_size)]
        
        for i, chunk in enumerate(chunks):
            try:
                logger.info(f"Processing chunk {i+1} of {len(chunks)}")
                rows_processed, docs_deleted = process_chunk(
                    bq_client, 
                    db, 
                    chunk, 
                    TABLE_ID, 
                    i+1, 
                    len(chunks)
                )
                total_processed += rows_processed
                logger.info(f"Processed {rows_processed} rows in chunk {i+1}")
            except Exception as e:
                logger.error(f"Error processing chunk {i+1}: {str(e)}", exc_info=True)
        
        logger.info(f"Total records processed: {total_processed}")
        return total_processed
    except Exception as e:
        logger.error(f"Error in process_firestore_to_bigquery: {str(e)}", exc_info=True)
        raise

def process_chunk(bq_client: bigquery.Client, db: firestore.Client, chunk_docs: list, 
                 table_id: str, chunk_number: int, total_chunks: int) -> Tuple[int, int]:
    """Process a chunk of documents from Firestore to BigQuery.
    
    Args:
        bq_client: BigQuery client
        db: Firestore client
        chunk_docs: List of Firestore documents to process
        table_id: BigQuery table ID
        chunk_number: Current chunk number
        total_chunks: Total number of chunks
        
    Returns:
        Tuple[int, int]: Number of rows processed and documents deleted
    """
    from datetime import datetime
    import time
    
    chunk_doc_refs = []
    rows_to_insert = []
    temp_table_id = f"{table_id}_temp_{chunk_number}"
    max_retries = 3
    retry_delay = 2
    
    try:
        # Transform documents to BigQuery format
        for doc in chunk_docs:
            data = doc.to_dict()
            chunk_doc_refs.append(doc.reference)
            
            # Transform the Firestore document into BigQuery format
            row = {
                'document_id': doc.id,
                'message_id': data.get('id'),
                'content': data.get('content'),
                'author': data.get('author'),
                'timestamp': data.get('timestamp'),
                'url': data.get('url'),
                'score': data.get('score'),
                'created_at': data.get('created_at').isoformat() if data.get('created_at') else None,
                'message_type': data.get('message_type'),
                'source': data.get('source', 'reddit'),
                'title': data.get('title'),
                'selftext': data.get('selftext'),
                'num_comments': data.get('num_comments'),
                'subreddit': data.get('subreddit'),
                'parent_id': data.get('parent_id'),
                'submission_id': data.get('submission_id'),
                'depth': data.get('depth'),
                'ingestion_timestamp': datetime.utcnow().isoformat()
            }
            rows_to_insert.append(row)
        
        # Filter out rows with [deleted] content
        filtered_rows = [row for row in rows_to_insert if row.get('content') != '[deleted]']
        
        if not filtered_rows:
            logger.info("No valid rows after filtering")
            return 0, 0
            
        # Create temporary table with same schema as the main table
        # Get schema from source table
        source_table = bq_client.get_table(table_id)
        temp_table = bigquery.Table(temp_table_id, schema=source_table.schema)
        temp_table = bq_client.create_table(temp_table, exists_ok=True)
        
        # Insert rows into temporary table with retry logic
        for attempt in range(max_retries):
            try:
                errors = bq_client.insert_rows_json(temp_table_id, filtered_rows)
                if not errors:
                    logger.info("Successfully inserted rows into temp table")
                    break
                else:
                    logger.warning(f"Errors inserting rows (attempt {attempt+1}): {errors}")
                    if attempt == max_retries - 1:
                        raise Exception(f"Failed to insert rows after {max_retries} attempts")
                    time.sleep(retry_delay)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Insert attempt {attempt+1} failed: {str(e)}")
                time.sleep(retry_delay)
        
        # Merge temporary table into main table
        merge_query = f"""
        MERGE `{table_id}` T
        USING `{temp_table_id}` S
        ON T.message_id = S.message_id
        WHEN MATCHED THEN
            UPDATE SET
                content = S.content,
                author = S.author,
                timestamp = S.timestamp,
                url = S.url,
                score = S.score,
                created_at = S.created_at,
                message_type = S.message_type,
                source = S.source,
                title = S.title,
                selftext = S.selftext,
                num_comments = S.num_comments,
                subreddit = S.subreddit,
                parent_id = S.parent_id,
                depth = S.depth,
                ingestion_timestamp = S.ingestion_timestamp
        WHEN NOT MATCHED THEN
            INSERT ROW
        """
        
        query_job = bq_client.query(merge_query)
        query_job.result()  # Wait for the query to complete
        affected_rows = query_job.num_dml_affected_rows
        
        # Delete processed documents from Firestore
        batch_size = 500
        docs_deleted = 0
        for i in range(0, len(chunk_doc_refs), batch_size):
            batch = db.batch()
            batch_docs = chunk_doc_refs[i:i + batch_size]
            
            for doc_ref in batch_docs:
                batch.delete(doc_ref)
                
            batch.commit()
            docs_deleted += len(batch_docs)
        
        return affected_rows, docs_deleted
        
    except Exception as e:
        logger.error(f"Error in process_chunk: {str(e)}", exc_info=True)
        raise
    finally:
        # Clean up temporary table
        bq_client.delete_table(temp_table_id, not_found_ok=True) 