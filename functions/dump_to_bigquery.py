import functions_framework
from google.cloud import firestore
from google.cloud import bigquery
from datetime import datetime, timedelta
import json

@functions_framework.cloud_event
def dump_to_bigquery(cloud_event):
    """Cloud Function triggered by Cloud Scheduler to dump Firestore data to BigQuery.
    Args:
        cloud_event (CloudEvent): The CloudEvent that triggered this function.
    """
    try:
        # Initialize Firestore and BigQuery clients
        db = firestore.Client()
        bq_client = bigquery.Client()
        
        # Get yesterday's date
        yesterday = datetime.utcnow() - timedelta(days=1)
        yesterday_start = datetime(yesterday.year, yesterday.month, yesterday.day)
        yesterday_end = yesterday_start + timedelta(days=1)
        
        # Query Firestore for yesterday's data
        docs = db.collection('raw_stock_data')\
            .where('timestamp', '>=', yesterday_start)\
            .where('timestamp', '<', yesterday_end)\
            .stream()
        
        # Prepare data for BigQuery
        rows_to_insert = []
        for doc in docs:
            data = doc.to_dict()
            row = {
                'document_id': doc.id,
                'timestamp': data['timestamp'].isoformat(),
                'source': data['source'],
                'raw_data': json.dumps(data['data'])
            }
            rows_to_insert.append(row)
        
        if not rows_to_insert:
            print('No data to insert')
            return
            
        # Define BigQuery table schema
        schema = [
            bigquery.SchemaField('document_id', 'STRING'),
            bigquery.SchemaField('timestamp', 'TIMESTAMP'),
            bigquery.SchemaField('source', 'STRING'),
            bigquery.SchemaField('raw_data', 'STRING')
        ]
        
        # Define table reference
        table_id = 'your-project.your_dataset.raw_stock_data'
        
        # Create table if it doesn't exist
        try:
            table = bq_client.get_table(table_id)
        except Exception:
            table = bigquery.Table(table_id, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field='timestamp'
            )
            table = bq_client.create_table(table)
        
        # Insert data
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)
        
        if errors:
            raise Exception(f'Errors inserting rows: {errors}')
            
        print(f'Successfully inserted {len(rows_to_insert)} rows')
        
    except Exception as e:
        print(f'Error: {str(e)}')
        raise 