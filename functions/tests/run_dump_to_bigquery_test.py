#!/usr/bin/env python
"""
Script to manually test the dump_to_bigquery function with a test table.
This script will:
1. Set up environment variables to use test collections/tables
2. Create test data in Firestore
3. Run the dump_to_bigquery function
4. Verify the results
"""

import os
import sys
import time
import uuid
from datetime import datetime, timezone
from google.cloud import firestore
from google.cloud import bigquery

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dump_to_bigquery import dump_to_bigquery

def setup_test_environment():
    """Set up the test environment with unique test collection."""
    # Create a unique test_run_id for this test to avoid conflicts
    test_run_id = str(uuid.uuid4())[:8]
    
    # Set environment variables for the test
    os.environ["FIRESTORE_STOCK_DATA_COLLECTION"] = f"test_stock_data_{test_run_id}"
    os.environ["BIGQUERY_DATASET_ID"] = "test_stock_data"
    
    # Get project ID from environment or fail
    if "PROJECT_ID" not in os.environ:
        project_id = input("Please enter your GCP project ID: ")
        os.environ["PROJECT_ID"] = project_id
    
    print(f"Using test collection: {os.environ['FIRESTORE_STOCK_DATA_COLLECTION']}")
    print(f"Using test dataset: {os.environ['BIGQUERY_DATASET_ID']}")
    print(f"Using project ID: {os.environ['PROJECT_ID']}")
    
    return test_run_id

def create_test_data(num_documents=5):
    """Create test data in Firestore."""
    print(f"Creating {num_documents} test documents in Firestore...")
    
    # Initialize Firestore client
    db = firestore.Client()
    collection_ref = db.collection(os.environ["FIRESTORE_STOCK_DATA_COLLECTION"])
    
    # Add test documents
    timestamp = datetime.now(timezone.utc)
    for i in range(num_documents):
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
    
    print(f"Created {num_documents} test documents in Firestore collection: {os.environ['FIRESTORE_STOCK_DATA_COLLECTION']}")
    
    # Verify documents were created
    docs = list(collection_ref.stream())
    print(f"Verified {len(docs)} documents in Firestore")
    
    return collection_ref

def verify_bigquery_results(test_run_id, expected_count):
    """Verify that data was correctly inserted into BigQuery."""
    print("Verifying BigQuery results...")
    
    # Initialize BigQuery client
    bq_client = bigquery.Client()
    table_id = f"{os.environ['PROJECT_ID']}.{os.environ['BIGQUERY_DATASET_ID']}.raw_messages"
    
    # Check the table exists
    try:
        table = bq_client.get_table(table_id)
        print(f"Table {table_id} exists")
    except Exception as e:
        print(f"Error getting table: {e}")
        return False
    
    # Check the data was inserted
    query = f"""
    SELECT COUNT(*) as count 
    FROM `{table_id}` 
    WHERE REGEXP_CONTAINS(document_id, r'test-doc-\\d')
    """
    
    try:
        job = bq_client.query(query)
        result = list(job.result())[0]
        
        # Should have expected_count rows
        if result.count == expected_count:
            print(f"✅ Found {result.count} rows in BigQuery (expected {expected_count})")
            return True
        else:
            print(f"❌ Found {result.count} rows in BigQuery (expected {expected_count})")
            return False
    except Exception as e:
        print(f"Error querying BigQuery: {e}")
        return False

def verify_firestore_deletion(collection_ref):
    """Verify that documents were deleted from Firestore."""
    print("Verifying Firestore deletion...")
    
    # Check that the data was deleted from Firestore
    docs = list(collection_ref.stream())
    if len(docs) == 0:
        print("✅ All documents were deleted from Firestore")
        return True
    else:
        print(f"❌ Found {len(docs)} documents in Firestore (expected 0)")
        return False

def run_test():
    """Run the test."""
    # Setup
    test_run_id = setup_test_environment()
    
    # Create test data
    collection_ref = create_test_data(num_documents=5)
    
    # Give Firestore a moment to settle
    print("Waiting for Firestore to settle...")
    time.sleep(2)
    
    # Run the function
    print("\nRunning dump_to_bigquery function...")
    try:
        result = dump_to_bigquery(None)
        print(f"Result: {result}")
    except Exception as e:
        print(f"Error running function: {e}")
        return
    
    # Verify results
    bigquery_success = verify_bigquery_results(test_run_id, 5)
    firestore_success = verify_firestore_deletion(collection_ref)
    
    # Print overall result
    if bigquery_success and firestore_success:
        print("\n✅ Test PASSED - Data was correctly moved from Firestore to BigQuery")
    else:
        print("\n❌ Test FAILED - Check the logs above for details")

if __name__ == "__main__":
    run_test() 