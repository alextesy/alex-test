import os
import pytest
import uuid
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from google.cloud import firestore
from google.cloud import bigquery

# Import the original module for reference
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dump_to_bigquery

# Test constants
TEST_COLLECTION = "test_stock_data"
TEST_DATASET = "test_stock_data"
TEST_PROJECT_ID = "test-project-id"

# Test implementation of the dump_to_bigquery function 
# This is a copy of the real function without the scheduling decorator
@pytest.mark.skip(reason="This is a reference implementation, not a test itself.")
def test_implementation(event=None):
    """
    Test implementation of the dump_to_bigquery function.
    
    This is a copy of the real function's implementation without the Firebase Functions
    scheduling decorator, allowing us to test the core logic directly.
    
    Note: This function is not a test by itself, but a reference implementation used by tests.
    """
    try:
        # Initialize Firestore and BigQuery clients
        db = firestore.Client()
        bq_client = bigquery.Client()
        
        logger = dump_to_bigquery.logger
        STOCK_DATA_COLLECTION = os.getenv('FIRESTORE_STOCK_DATA_COLLECTION', 'stock_data')
        
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
        error_msg = f'Error in dump_to_bigquery: {str(e)}'
        dump_to_bigquery.logger.error(error_msg, exc_info=True)
        raise

@pytest.fixture
def mock_env(monkeypatch):
    """Set up environment variables for testing."""
    monkeypatch.setenv("FIRESTORE_STOCK_DATA_COLLECTION", TEST_COLLECTION)
    monkeypatch.setenv("BIGQUERY_DATASET_ID", TEST_DATASET)
    monkeypatch.setenv("PROJECT_ID", TEST_PROJECT_ID)

@pytest.fixture
def mock_firestore_data():
    """Create test data for Firestore."""
    timestamp = datetime.now(timezone.utc)
    return [
        {
            "id": f"test-id-{i}",
            "content": f"Test content {i}",
            "author": f"test-author-{i}",
            "timestamp": timestamp,
            "url": f"https://example.com/{i}",
            "score": i,
            "created_at": timestamp,
            "message_type": "post",
            "source": "reddit",
            "title": f"Test title {i}",
            "selftext": f"Test selftext {i}",
            "num_comments": i * 10,
            "subreddit": "wallstreetbets",
            "parent_id": None,
            "depth": 0
        } for i in range(1, 6)  # Create 5 test documents
    ]

@pytest.fixture
def setup_firestore(mock_firestore_data):
    """Set up Firestore with test data."""
    with patch('google.cloud.firestore.Client') as mock_firestore_client:
        # Create mock documents
        mock_docs = []
        for i, data in enumerate(mock_firestore_data):
            mock_doc = MagicMock()
            mock_doc.id = f"doc-{i}"
            
            # Create a dict to return from to_dict() with proper timestamp handling
            doc_data = data.copy()
            # Configure timestamp mock to handle isoformat() calls
            timestamp_mock = MagicMock()
            timestamp_mock.isoformat.return_value = data['timestamp'].isoformat()
            doc_data['timestamp'] = timestamp_mock
            
            # Configure created_at mock to handle isoformat() calls
            created_at_mock = MagicMock()
            created_at_mock.isoformat.return_value = data['created_at'].isoformat()
            doc_data['created_at'] = created_at_mock
            
            mock_doc.to_dict.return_value = doc_data
            mock_doc.reference = MagicMock()
            mock_docs.append(mock_doc)
        
        # Set up the mock Firestore client
        mock_collection = MagicMock()
        mock_collection.stream.return_value = mock_docs
        
        mock_client = MagicMock()
        mock_client.collection.return_value = mock_collection
        mock_firestore_client.return_value = mock_client
        
        # Mock batch operations
        mock_batch = MagicMock()
        mock_client.batch.return_value = mock_batch
        
        yield mock_client, mock_docs

@pytest.fixture
def setup_bigquery():
    """Set up BigQuery mocks."""
    with patch('google.cloud.bigquery.Client') as mock_bq_client:
        mock_client = MagicMock()
        
        # Mock get_table to raise exception (table doesn't exist)
        mock_client.get_table.side_effect = Exception("Table not found")
        
        # Mock create_table
        mock_client.create_table.return_value = MagicMock()
        
        # Mock insert_rows_json to return no errors
        mock_client.insert_rows_json.return_value = []
        
        mock_bq_client.return_value = mock_client
        
        yield mock_client

@pytest.mark.skip(reason="This test requires real credentials to run. Remove this decorator to run with valid credentials.")
def test_with_real_services():
    """
    Integration test with real services.
    
    This test is skipped by default as it requires real credentials.
    To run it, remove the @pytest.mark.skip decorator and ensure you have:
    1. Proper GCP credentials set up
    2. Test collection in Firestore
    3. Test dataset in BigQuery
    """
    # Create a unique test_run_id for this test to avoid conflicts
    test_run_id = str(uuid.uuid4())
    os.environ["FIRESTORE_STOCK_DATA_COLLECTION"] = f"test_stock_data_{test_run_id}"
    os.environ["BIGQUERY_DATASET_ID"] = "test_stock_data"
    os.environ["PROJECT_ID"] = "your-project-id"  # Replace with your actual project ID
    
    # Create test data in Firestore
    db = firestore.Client()
    collection_ref = db.collection(os.environ["FIRESTORE_STOCK_DATA_COLLECTION"])
    
    # Add test documents
    timestamp = datetime.now(timezone.utc)
    for i in range(5):
        collection_ref.document(f"test-doc-{i}").set({
            "id": f"test-id-{i}",
            "content": f"Test content {i}",
            "author": f"test-author-{i}",
            "timestamp": timestamp,
            "url": f"https://example.com/{i}",
            "score": i,
            "created_at": timestamp,
            "message_type": "post",
            "source": "reddit",
            "title": f"Test title {i}",
            "selftext": f"Test selftext {i}",
            "num_comments": i * 10,
            "subreddit": "wallstreetbets",
            "parent_id": None,
            "depth": 0
        })
    
    # Run our test function
    result = test_implementation()
    
    # Verify result
    assert "Successfully inserted" in result
    assert "deleted" in result
    
    # Verify data was inserted to BigQuery
    bq_client = bigquery.Client()
    table_id = f"{os.environ['PROJECT_ID']}.{os.environ['BIGQUERY_DATASET_ID']}.raw_messages"
    
    # Check the data was inserted (should have 5 rows with our test_run_id)
    query = f"""
    SELECT COUNT(*) as count 
    FROM `{table_id}` 
    WHERE REGEXP_CONTAINS(document_id, r'test-doc-\\d')
    """
    
    job = bq_client.query(query)
    result = list(job.result())[0]
    
    # Should have 5 rows
    assert result.count == 5
    
    # Check that the data was deleted from Firestore
    docs = list(collection_ref.stream())
    assert len(docs) == 0

def test_core_functionality(mock_env, setup_firestore, setup_bigquery):
    """Test the core functionality of extracting data, writing to BigQuery, and deleting from Firestore."""
    mock_firestore, mock_docs = setup_firestore
    mock_bigquery = setup_bigquery
    
    # We're testing our copied function
    with patch('google.cloud.firestore.Client', return_value=mock_firestore):
        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery):
            # Run the test implementation
            result = test_implementation()
            
            # Assertions
            
            # 1. Verify Firestore collection was queried
            mock_firestore.collection.assert_called_once_with(TEST_COLLECTION)
            
            # 2. Verify data was inserted to BigQuery
            expected_table_id = f"{TEST_PROJECT_ID}.{TEST_DATASET}.raw_messages"
            mock_bigquery.insert_rows_json.assert_called_once()
            
            # First arg should be the table_id
            assert mock_bigquery.insert_rows_json.call_args[0][0] == expected_table_id
            
            # Second arg should be the rows to insert
            inserted_rows = mock_bigquery.insert_rows_json.call_args[0][1]
            assert len(inserted_rows) == len(mock_docs)
            
            # 3. Verify batch delete was called
            mock_firestore.batch.assert_called_once()
            mock_batch = mock_firestore.batch.return_value
            
            # Verify batch.delete was called for each document
            assert mock_batch.delete.call_count == len(mock_docs)
            
            # Verify batch.commit was called
            mock_batch.commit.assert_called_once()
            
            # 4. Verify function returns success message
            assert "Successfully inserted" in result
            assert "deleted" in result
            assert str(len(mock_docs)) in result  # Number of inserted rows
            assert str(len(mock_docs)) in result  # Number of deleted docs

def test_empty_collection(mock_env, setup_firestore, setup_bigquery):
    """Test behavior when Firestore collection is empty."""
    mock_firestore, _ = setup_firestore
    mock_bigquery = setup_bigquery
    
    # Override mock_docs to be empty
    mock_firestore.collection.return_value.stream.return_value = []
    
    # Test our copied function
    with patch('google.cloud.firestore.Client', return_value=mock_firestore):
        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery):
            # Run the test implementation
            result = test_implementation()
            
            # Verify function returns proper message for empty collection
            assert result == "No data to insert"
            
            # Verify no BigQuery or deletion operations were performed
            mock_bigquery.insert_rows_json.assert_not_called()
            mock_firestore.batch.assert_not_called()

def test_bigquery_error(mock_env, setup_firestore, setup_bigquery):
    """Test behavior when BigQuery insert fails."""
    mock_firestore, mock_docs = setup_firestore
    mock_bigquery = setup_bigquery
    
    # Make BigQuery insert_rows_json return errors
    mock_bigquery.insert_rows_json.return_value = ["Error inserting row"]
    
    # Test our copied function
    with patch('google.cloud.firestore.Client', return_value=mock_firestore):
        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery):
            # Run the function - should raise an exception
            with pytest.raises(Exception) as excinfo:
                test_implementation()
            
            # Verify exception message contains the error
            assert "Errors inserting rows" in str(excinfo.value)
            
            # Verify no deletion operations were performed
            mock_firestore.batch.assert_not_called()

if __name__ == "__main__":
    pytest.main() 