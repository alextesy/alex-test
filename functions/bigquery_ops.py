import logging
from datetime import datetime
from google.cloud import bigquery
from google.cloud import firestore
import time
from typing import Tuple

logger = logging.getLogger(__name__)

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

def create_temp_table(bq_client: bigquery.Client, source_table_id: str, temp_table_id: str) -> None:
    """Create a temporary table with the same schema as the source table.
    
    Args:
        bq_client: BigQuery client
        source_table_id: ID of the source table to copy schema from
        temp_table_id: ID of the temporary table to create
    """
    logger.info(f"Creating temporary table {temp_table_id}")
    
    # Get schema from source table
    source_table = bq_client.get_table(source_table_id)
    temp_table = bigquery.Table(temp_table_id, schema=source_table.schema)
    
    # Create the table
    temp_table = bq_client.create_table(temp_table, exists_ok=True)
    logger.info(f"Temporary table created: {temp_table_id}")
    
    # Verify table exists by getting it
    try:
        bq_client.get_table(temp_table_id)
        logger.info(f"Verified temporary table {temp_table_id} exists and is ready")
    except Exception as e:
        error_msg = f"Failed to verify temporary table {temp_table_id} exists: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

def filter_deleted_rows(rows: list) -> list:
    """Filter out rows where content is '[deleted]' and existing content is not '[deleted]'.
    
    Args:
        rows: List of rows to filter
        
    Returns:
        list: Filtered rows
    """
    return [row for row in rows if row['content'] != '[deleted]']

def execute_merge_operation(bq_client: bigquery.Client, target_table_id: str, temp_table_id: str) -> int:
    """Execute merge operation between temporary and target tables.
    
    Args:
        bq_client: BigQuery client
        target_table_id: ID of the target table
        temp_table_id: ID of the temporary table
        
    Returns:
        int: Number of affected rows
    """
    merge_query = f"""
    MERGE `{target_table_id}` T
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
    return query_job.num_dml_affected_rows

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

def process_chunk(bq_client: bigquery.Client, db: firestore.Client, chunk_docs: list, table_id: str, chunk_number: int, total_chunks: int) -> Tuple[int, int]:
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
    temp_table_id = f"{table_id}_temp_{chunk_number}"
    max_retries = 3
    retry_delay = 2  # seconds
    
    logger.info(f"Processing chunk {chunk_number} of {total_chunks}")
    
    try:
        # Transform documents
        for doc in chunk_docs:
            chunk_doc_refs.append(doc.reference)
            rows_to_insert.append(transform_firestore_doc(doc))
        
        logger.info(f"Processing {len(rows_to_insert)} rows for chunk {chunk_number}")
        
        # Filter out [deleted] rows
        filtered_rows = filter_deleted_rows(rows_to_insert)
        logger.info(f"After filtering {len(filtered_rows)} rows for chunk {chunk_number}")

        if not filtered_rows:
            logger.info("No valid rows to process after filtering [deleted] content")
            return 0, 0
            
        # Create and populate temporary table
        create_temp_table(bq_client, table_id, temp_table_id)
        
        # Retry logic for inserting rows
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting to insert rows into temp table (attempt {attempt + 1}/{max_retries})")
                errors = bq_client.insert_rows_json(temp_table_id, filtered_rows)
                if not errors:
                    logger.info("Successfully inserted rows into temp table")
                    break
                else:
                    error_msg = f"Error inserting rows (attempt {attempt + 1}): {errors}"
                    logger.warning(error_msg)
                    if attempt == max_retries - 1:
                        raise Exception(error_msg)
                    time.sleep(retry_delay)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Insert attempt {attempt + 1} failed: {str(e)}")
                time.sleep(retry_delay)
        
        # Execute merge operation
        affected_rows = execute_merge_operation(bq_client, table_id, temp_table_id)
        logger.info(f"Successfully processed {affected_rows} rows")
        
        # Delete from Firestore
        docs_deleted = delete_firestore_docs(db, chunk_doc_refs)
        
        logger.info(f"Completed chunk {chunk_number}. Processed: {affected_rows}, Deleted: {docs_deleted}")
        return affected_rows, docs_deleted
        
    except Exception as e:
        error_msg = f'Error processing chunk {chunk_number}: {e}'
        logger.error(error_msg)
        raise Exception(error_msg)
    finally:
        # Clean up temporary table
        bq_client.delete_table(temp_table_id, not_found_ok=True) 