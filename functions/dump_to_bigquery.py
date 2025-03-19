import os
import logging
from firebase_functions import scheduler_fn, options
from google.cloud import firestore
from google.cloud import bigquery
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Get collection name from environment
STOCK_DATA_COLLECTION = os.getenv('FIRESTORE_STOCK_DATA_COLLECTION', 'stock_data')

@scheduler_fn.on_schedule(
    schedule="0 * * * *",  # Run every hour by default
    memory=options.MemoryOption.GB_1,
    timeout_sec=540  # 9 minutes
)
def firestore_to_bigquery_etl(event: scheduler_fn.ScheduledEvent) -> str:
    """Cloud Function triggered hourly to dump all Firestore stock data to BigQuery and delete from Firestore.
    
    This function performs a complete ETL process:
    1. Extracts all data from Firestore
    2. Transforms it to fit BigQuery schema
    3. Loads it into BigQuery
    4. Deletes the processed data from Firestore
    
    Args:
        event (ScheduledEvent): The event that triggered this function.
    Returns:
        str: Status message
    """
    try:
        # Initialize Firestore and BigQuery clients
        db = firestore.Client()
        bq_client = bigquery.Client()
        
        logger.info("Fetching all data from Firestore collection")
        
        # Query Firestore for all data
        collection_ref = db.collection(STOCK_DATA_COLLECTION)
        docs = collection_ref.stream()
        
        # Keep track of document references for later deletion
        doc_refs = []
        
        # Prepare data for BigQuery
        rows_to_insert = []
        for doc in docs:
            doc_refs.append(doc.reference)
            data = doc.to_dict()
            # Flatten and prepare the data for BigQuery
            row = {
                'document_id': doc.id,
                'message_id': data.get('id'),
                'content': data.get('content'),
                'author': data.get('author'),
                'timestamp': data.get('timestamp').isoformat() if data.get('timestamp') else None,
                'url': data.get('url'),
                'score': data.get('score'),
                'created_at': data.get('created_at').isoformat() if data.get('created_at') else None,
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
            rows_to_insert.append(row)
        
        if not rows_to_insert:
            logger.info('No data to insert')
            return 'No data to insert'
            
        # Define BigQuery table schema
        schema = [
            bigquery.SchemaField('document_id', 'STRING'),
            bigquery.SchemaField('message_id', 'STRING'),
            bigquery.SchemaField('content', 'STRING'),
            bigquery.SchemaField('author', 'STRING'),
            bigquery.SchemaField('timestamp', 'TIMESTAMP'),
            bigquery.SchemaField('url', 'STRING'),
            bigquery.SchemaField('score', 'INTEGER'),
            bigquery.SchemaField('created_at', 'TIMESTAMP'),
            bigquery.SchemaField('message_type', 'STRING'),
            bigquery.SchemaField('source', 'STRING'),
            bigquery.SchemaField('title', 'STRING'),
            bigquery.SchemaField('selftext', 'STRING'),
            bigquery.SchemaField('num_comments', 'INTEGER'),
            bigquery.SchemaField('subreddit', 'STRING'),
            bigquery.SchemaField('parent_id', 'STRING'),
            bigquery.SchemaField('depth', 'INTEGER'),
            bigquery.SchemaField('ingestion_timestamp', 'TIMESTAMP')
        ]
        
        # Get project ID from environment
        project_id = os.getenv('PROJECT_ID')
        if not project_id:
            raise ValueError("PROJECT_ID environment variable is not set")
            
        # Define table reference
        dataset_id = os.getenv('BIGQUERY_DATASET_ID', 'stock_data')
        table_id = f"{project_id}.{dataset_id}.raw_messages"
        
        # Create table if it doesn't exist
        try:
            table = bq_client.get_table(table_id)
        except Exception:
            table = bigquery.Table(table_id, schema=schema)
            # Partition by timestamp for better query performance
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field='timestamp'
            )
            # Cluster by commonly queried fields
            table.clustering_fields = ['source', 'subreddit', 'message_type']
            table = bq_client.create_table(table)
            logger.info(f"Created new table {table_id}")
        
        # Insert data
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)
        
        if errors:
            error_msg = f'Errors inserting rows: {errors}'
            logger.error(error_msg)
            raise Exception(error_msg)
            
        # If insertion was successful, delete documents from Firestore
        batch_size = 500  # Firestore allows up to 500 operations per batch
        total_deleted = 0
        
        # Process deletions in batches
        for i in range(0, len(doc_refs), batch_size):
            batch = db.batch()
            batch_docs = doc_refs[i:i+batch_size]
            
            for doc_ref in batch_docs:
                batch.delete(doc_ref)
                
            batch.commit()
            total_deleted += len(batch_docs)
            logger.info(f"Deleted batch of {len(batch_docs)} documents from Firestore")
            
        success_msg = f'Successfully inserted {len(rows_to_insert)} rows to BigQuery and deleted {total_deleted} documents from Firestore'
        logger.info(success_msg)
        return success_msg
        
    except Exception as e:
        error_msg = f'Error in firestore_to_bigquery_etl: {str(e)}'
        logger.error(error_msg, exc_info=True)
        raise 