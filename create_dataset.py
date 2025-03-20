from google.cloud import bigquery
from google.api_core import exceptions

def create_dataset_and_table():
    client = bigquery.Client()
    dataset_id = f"{client.project}.reddit_data"
    table_id = f"{dataset_id}.raw_messages"
    
    try:
        # Create the dataset
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        
        # Set access entries for the dataset
        service_account = f"{client.project}@appspot.gserviceaccount.com"
        entries = list(dataset.access_entries)
        
        # Add owner access for the service account
        entries.append(
            bigquery.AccessEntry(
                role="OWNER",
                entity_type="userByEmail",
                entity_id=service_account
            )
        )
        
        dataset.access_entries = entries
        
        # Create the dataset with the access entries
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"Created dataset {client.project}.reddit_data with access entries")
        
        # Define table schema
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
        
        # Create table
        table = bigquery.Table(table_id, schema=schema)
        
        # Set table partitioning
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field='timestamp'
        )
        
        # Set clustering fields
        table.clustering_fields = ['source', 'subreddit', 'message_type']
        
        # Create the table
        table = client.create_table(table, exists_ok=True)
        print(f"Created table {table_id}")
        
    except exceptions.Conflict:
        print(f"Dataset {dataset_id} already exists")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    create_dataset_and_table() 