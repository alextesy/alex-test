import os
import logging
from typing import List, Dict, Any, TypeVar, Generic, Optional, Type
from datetime import datetime
import json
import time

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import NotFound as GoogleApiNotFound
import sqlalchemy

from src.utils.json_utils import safe_json_dumps

logger = logging.getLogger(__name__)

# Define type variables for generic types
T = TypeVar('T')

class BigQueryManager:
    """
    BigQuery manager class for data operations.
    """
    
    _instance = None
    
    def __new__(cls):
        """Singleton pattern to ensure only one instance exists."""
        if cls._instance is None:
            cls._instance = super(BigQueryManager, cls).__new__(cls)
            cls._instance.initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the BigQuery manager."""
        if not getattr(self, 'initialized', False):
            self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT_ID')
            self.dataset_id = os.getenv('BIGQUERY_DATASET')
            self.client = None
            self.tables = {}
            self.schemas = {}
            self.initialized = True
    
    def connect(self) -> bigquery.Client:
        """
        Create a BigQuery client connection.
        
        Returns:
            BigQuery client
        """
        if self.client is None:
            self.client = bigquery.Client(project=self.project_id)
            logger.info(f"Connected to BigQuery dataset {self.dataset_id} in project {self.project_id}")
            
        return self.client
    
    def setup_tables(self):
        """
        Set up BigQuery tables for stock data.
        """
        logger.info("Setting up BigQuery tables for stock data")
        
        client = self.connect()
        
        # Define schemas for each table
        self.schemas['stock_mentions'] = [
            bigquery.SchemaField("message_id", "STRING"),
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("author", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("subreddit", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("score", "INTEGER"),
            bigquery.SchemaField("message_type", "STRING"),
            bigquery.SchemaField("sentiment_compound", "FLOAT"),
            bigquery.SchemaField("sentiment_positive", "FLOAT"),
            bigquery.SchemaField("sentiment_negative", "FLOAT"),
            bigquery.SchemaField("sentiment_neutral", "FLOAT"),
            bigquery.SchemaField("signals", "JSON"),
            bigquery.SchemaField("context", "STRING"),
            bigquery.SchemaField("confidence", "FLOAT"),
            bigquery.SchemaField("etl_timestamp", "TIMESTAMP")
        ]
        
        self.schemas['stock_daily_summary'] = [
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("mention_count", "INTEGER"),
            bigquery.SchemaField("avg_sentiment", "FLOAT"),
            bigquery.SchemaField("weighted_sentiment", "FLOAT"),
            bigquery.SchemaField("buy_signals", "INTEGER"),
            bigquery.SchemaField("sell_signals", "INTEGER"),
            bigquery.SchemaField("hold_signals", "INTEGER"),
            bigquery.SchemaField("price_targets", "JSON"),
            bigquery.SchemaField("news_signals", "INTEGER"),
            bigquery.SchemaField("earnings_signals", "INTEGER"),
            bigquery.SchemaField("technical_signals", "INTEGER"),
            bigquery.SchemaField("options_signals", "INTEGER"),
            bigquery.SchemaField("avg_confidence", "FLOAT"),
            bigquery.SchemaField("high_conf_sentiment", "FLOAT"),
            bigquery.SchemaField("top_contexts", "JSON"),
            bigquery.SchemaField("subreddits", "JSON"),
            bigquery.SchemaField("etl_timestamp", "TIMESTAMP")
        ]
        
        self.schemas['stock_hourly_summary'] = [
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("hour_start", "TIMESTAMP"),
            bigquery.SchemaField("mention_count", "INTEGER"),
            bigquery.SchemaField("avg_sentiment", "FLOAT"),
            bigquery.SchemaField("weighted_sentiment", "FLOAT"),
            bigquery.SchemaField("buy_signals", "INTEGER"),
            bigquery.SchemaField("sell_signals", "INTEGER"),
            bigquery.SchemaField("hold_signals", "INTEGER"),
            bigquery.SchemaField("avg_confidence", "FLOAT"),
            bigquery.SchemaField("subreddits", "JSON"),
            bigquery.SchemaField("etl_timestamp", "TIMESTAMP")
        ]
        
        self.schemas['stock_weekly_summary'] = [
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("week_start", "DATE"),
            bigquery.SchemaField("mention_count", "INTEGER"),
            bigquery.SchemaField("avg_sentiment", "FLOAT"),
            bigquery.SchemaField("weighted_sentiment", "FLOAT"),
            bigquery.SchemaField("buy_signals", "INTEGER"),
            bigquery.SchemaField("sell_signals", "INTEGER"),
            bigquery.SchemaField("hold_signals", "INTEGER"),
            bigquery.SchemaField("price_targets", "JSON"),
            bigquery.SchemaField("news_signals", "INTEGER"),
            bigquery.SchemaField("earnings_signals", "INTEGER"),
            bigquery.SchemaField("technical_signals", "INTEGER"),
            bigquery.SchemaField("options_signals", "INTEGER"),
            bigquery.SchemaField("avg_confidence", "FLOAT"),
            bigquery.SchemaField("daily_breakdown", "JSON"),
            bigquery.SchemaField("subreddits", "JSON"),
            bigquery.SchemaField("etl_timestamp", "TIMESTAMP")
        ]
        
        # Create dataset if it doesn't exist
        dataset_ref = client.dataset(self.dataset_id)
        try:
            client.get_dataset(dataset_ref)
        except NotFound:
            # Dataset does not exist, create it
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = os.getenv('GCP_REGION', 'US')
            client.create_dataset(dataset)
            logger.info(f"Created BigQuery dataset {self.dataset_id}")
        
        # Create tables if they don't exist
        for table_name, schema in self.schemas.items():
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            table = bigquery.Table(table_id, schema=schema)
            
            try:
                client.get_table(table)
                logger.info(f"Table {table_name} already exists")
            except NotFound:
                # Table does not exist, create it
                table = client.create_table(table)
                logger.info(f"Created table {table_id}")
    
    def check_stock_mention_exists(self, message_id: str, ticker: str) -> bool:
        """
        Check if a stock mention already exists in BigQuery.
        
        Args:
            message_id: ID of the message
            ticker: Stock ticker
            
        Returns:
            bool: True if the mention exists, False otherwise
        """
        client = self.connect()
        query = f"""
        SELECT COUNT(*) as count
        FROM `{self.project_id}.{self.dataset_id}.stock_mentions`
        WHERE message_id = @message_id AND ticker = @ticker
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("message_id", "STRING", message_id),
                bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
            ]
        )
        
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        
        for row in results:
            return row.count > 0
        
        return False
    
    def bulk_insert_stock_mentions(self, mentions: List[Dict[str, Any]]):
        """
        Bulk insert stock mentions to BigQuery, skipping any that already exist.
        
        Args:
            mentions: List of stock mention dictionaries
        """
        if not mentions:
            return
        
        client = self.connect()
        table_id = f"{self.project_id}.{self.dataset_id}.stock_mentions"
        
        # Instead of checking one by one, batch query for existing records
        message_ticker_pairs = [(mention['message_id'], mention['ticker']) for mention in mentions]
        
        # Prepare unique keys for batch existence check
        unique_keys = set()
        for message_id, ticker in message_ticker_pairs:
            unique_keys.add(f"{message_id}_{ticker}")
        
        # Check which records already exist in a single query
        existing_records = set()
        
        if unique_keys:
            # Split into chunks to avoid overly large queries
            chunk_size = 1000
            key_chunks = [list(unique_keys)[i:i + chunk_size] for i in range(0, len(unique_keys), chunk_size)]
            
            for chunk in key_chunks:
                # Create a placeholder string for the IN clause
                placeholders = ", ".join([f"('{k.split('_')[0]}', '{k.split('_')[1]}')" for k in chunk])
                
                query = f"""
                SELECT CONCAT(message_id, '_', ticker) as unique_key
                FROM `{self.project_id}.{self.dataset_id}.stock_mentions`
                WHERE (message_id, ticker) IN ({placeholders})
                """
                
                logger.info(f"Checking for {len(chunk)} existing records")
                query_job = client.query(query)
                
                for row in query_job:
                    existing_records.add(row.unique_key)
                
                logger.info(f"Found {len(existing_records)} already existing records")
        
        # Filter out records that already exist
        new_mentions = []
        for mention in mentions:
            unique_key = f"{mention['message_id']}_{mention['ticker']}"
            if unique_key not in existing_records:
                # Convert datetime objects to ISO format strings for JSON serialization
                for key, value in mention.items():
                    if isinstance(value, datetime):
                        mention[key] = value.isoformat()
                
                # Convert signals to string if it's not already
                if 'signals' in mention and mention['signals'] is not None:
                    if not isinstance(mention['signals'], str):
                        mention['signals'] = safe_json_dumps(mention['signals'])
                new_mentions.append(mention)
        
        if not new_mentions:
            logger.info("No new stock mentions to insert")
            return
        
        # Insert in batches rather than all at once
        batch_size = 1000
        mention_batches = [new_mentions[i:i + batch_size] for i in range(0, len(new_mentions), batch_size)]
        
        total_inserted = 0
        for batch_index, batch in enumerate(mention_batches):
            try:
                # Insert records into BigQuery
                logger.info(f"Inserting batch {batch_index + 1}/{len(mention_batches)} with {len(batch)} records")
                errors = client.insert_rows_json(table_id, batch)
                
                if errors:
                    logger.error(f"Errors inserting stock mentions batch {batch_index + 1}: {errors}")
                else:
                    total_inserted += len(batch)
                    logger.info(f"Successfully inserted batch {batch_index + 1} ({total_inserted}/{len(new_mentions)} total)")
            except Exception as e:
                logger.error(f"Error inserting stock mentions batch {batch_index + 1} into BigQuery: {str(e)}")
                # Log the first record for debugging
                if batch:
                    logger.error(f"Sample record causing error: {batch[0]}")
                # Continue with next batch instead of failing completely
                continue
        
        logger.info(f"Successfully inserted {total_inserted} stock mentions to BigQuery (out of {len(new_mentions)} new mentions)")


class BaseBigQueryManager(Generic[T]):
    """
    Base class for managing BigQuery data operations for specific summary types.
    """
    
    def __init__(self, table_name: str, ticker_field: str, date_field: str):
        """
        Initialize the base BigQuery data manager.
        
        Args:
            table_name: Name of the BigQuery table
            ticker_field: Name of the ticker field
            date_field: Name of the date field
        """
        self.bq_manager = BigQueryManager()
        self.table_name = table_name
        self.ticker_field = ticker_field
        self.date_field = date_field
        self.client = self.bq_manager.connect()
        self.project_id = self.bq_manager.project_id
        self.dataset_id = self.bq_manager.dataset_id
    
    def get_existing_record(self, ticker: str, date_value: str) -> Optional[Dict[str, Any]]:
        """
        Check if a record already exists in BigQuery.
        
        Args:
            ticker: Stock ticker
            date_value: Date value for the record
            
        Returns:
            Optional[Dict[str, Any]]: The existing record or None
        """
        query = f"""
        SELECT *
        FROM `{self.project_id}.{self.dataset_id}.{self.table_name}`
        WHERE {self.ticker_field} = @ticker AND {self.date_field} = @date_value
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
                bigquery.ScalarQueryParameter("date_value", "STRING", date_value),
            ]
        )
        
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result()
        
        for row in results:
            return dict(row.items())
        
        return None
    
    def query_with_deduplicated_messages(self, source_table: str, conditions: str = None) -> List[Dict[str, Any]]:
        """
        Query data from a source table with deduplication based on message_id.
        This is useful when joining with message tables that might have duplicates.
        
        Args:
            source_table: Name of the source table (e.g., "raw_messages")
            conditions: Additional SQL WHERE conditions
            
        Returns:
            List[Dict[str, Any]]: List of records with deduplicated messages
        """
        where_clause = f"WHERE {conditions}" if conditions else ""
        
        query = f"""
        WITH DedupMessages AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (PARTITION BY message_id ORDER BY etl_timestamp DESC) as row_num
            FROM 
                `{self.project_id}.{self.dataset_id}.{source_table}`
        )
        SELECT t.*
        FROM `{self.project_id}.{self.dataset_id}.{self.table_name}` t
        JOIN DedupMessages d ON t.message_id = d.message_id
        WHERE d.row_num = 1
        {where_clause}
        """
        
        logger.info(f"Executing query to fetch data from {self.table_name} with deduplicated messages from {source_table}")
        query_job = self.client.query(query)
        
        results = []
        for row in query_job:
            results.append(dict(row.items()))
        
        logger.info(f"Retrieved {len(results)} records with deduplicated messages")
        return results
    
    def insert_or_update_records(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert or update records in BigQuery using MERGE operation.
        
        Args:
            records: List of records to insert/update
            
        Returns:
            int: Number of records updated/inserted
        """
        if not records:
            return 0
        
        # Instead of creating SQL strings with potential Unicode issues,
        # use BigQuery's parametrized queries with JSON data
        
        # Convert records to JSON format for insert - with proper datetime handling
        json_rows = []
        for record in records:
            # Create a copy of the record to avoid modifying the original
            record_copy = record.copy()
            
            # Convert any datetime objects to ISO format strings for JSON serialization
            for key, value in record_copy.items():
                if isinstance(value, datetime):
                    record_copy[key] = value.isoformat()
                elif isinstance(value, (dict, list)) and not isinstance(value, str):
                    # Safely handle nested structures using the safe_json_dumps utility
                    record_copy[key] = safe_json_dumps(value)
            
            json_rows.append(json.dumps(record_copy))
        
        # Create a temporary table with our records
        temp_table_id = f"{self.table_name}_temp_{int(time.time())}"
        schema = self._get_table_schema()
        
        # Create temp table
        temp_table = bigquery.Table(f"{self.project_id}.{self.dataset_id}.{temp_table_id}", schema=schema)
        try:
            self.client.create_table(temp_table, exists_ok=False)
            logger.info(f"Created temporary table {temp_table_id}")
            
            # Load JSON data to temp table
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            
            # Convert records to newline-delimited JSON
            json_data = "\n".join(json_rows)
            load_job = self.client.load_table_from_json(
                json.loads("[" + ",".join(json_rows) + "]"), 
                f"{self.project_id}.{self.dataset_id}.{temp_table_id}", 
                job_config=job_config
            )
            load_job.result()  # Wait for load to complete
            
            # Define key fields for merge operation
            key_fields = [self.ticker_field, self.date_field]
            key_conditions = " AND ".join([f"T.{field} = S.{field}" for field in key_fields])
            
            # Get update columns (excluding key fields)
            update_columns = [col for col in records[0].keys() if col not in key_fields]
            update_clause = ", ".join([f"{col} = S.{col}" for col in update_columns])
            
            # Build field lists for insert
            all_fields = ", ".join(records[0].keys())
            source_fields = ", ".join([f"S.{field}" for field in records[0].keys()])
            
            # Execute MERGE operation using the temp table
            merge_query = f"""
            MERGE `{self.project_id}.{self.dataset_id}.{self.table_name}` T
            USING `{self.project_id}.{self.dataset_id}.{temp_table_id}` S
            ON {key_conditions}
            WHEN MATCHED THEN
              UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
              INSERT({all_fields})
              VALUES({source_fields})
            """
            
            query_job = self.client.query(merge_query)
            result = query_job.result()
            
            # Count affected rows - not directly available from BigQuery merge,
            # so we'll query the stats
            return len(records)
            
        finally:
            # Clean up the temporary table
            try:
                self.client.delete_table(f"{self.project_id}.{self.dataset_id}.{temp_table_id}")
                logger.info(f"Deleted temporary table {temp_table_id}")
            except Exception as e:
                logger.warning(f"Failed to delete temporary table: {str(e)}")

    def _get_table_schema(self) -> List[bigquery.SchemaField]:
        """
        Get the schema for the current table.
        
        Returns:
            List of SchemaField objects representing the table schema
        """
        table_ref = self.client.dataset(self.dataset_id).table(self.table_name)
        table = self.client.get_table(table_ref)
        return table.schema
    
    def save_records(self, records: List[T]) -> int:
        """
        Save records to BigQuery, handling JSON conversion.
        
        Args:
            records: List of record objects
            
        Returns:
            int: Number of records saved
        """
        if not records:
            return 0
            
        # Convert records to dictionaries
        record_dicts = []
        for record in records:
            # Handle both dict objects and custom class objects
            if hasattr(record, 'to_dict'):
                record_dict = record.to_dict()
            elif isinstance(record, dict):
                record_dict = record
            else:
                logger.error(f"Unsupported record type: {type(record)}")
                continue
                
            # Add etl_timestamp if not present
            if 'etl_timestamp' not in record_dict:
                record_dict['etl_timestamp'] = datetime.utcnow()
                
            record_dicts.append(record_dict)
            
        # Insert or update records
        return self.insert_or_update_records(record_dicts)


class DailyBigQueryManager(BaseBigQueryManager):
    """Manager for daily summary BigQuery operations."""
    
    def __init__(self):
        super().__init__('stock_daily_summary', 'ticker', 'date')


class HourlyBigQueryManager(BaseBigQueryManager):
    """Manager for hourly summary BigQuery operations."""
    
    def __init__(self):
        super().__init__('stock_hourly_summary', 'ticker', 'hour_start')


class WeeklyBigQueryManager(BaseBigQueryManager):
    """Manager for weekly summary BigQuery operations."""
    
    def __init__(self):
        super().__init__('stock_weekly_summary', 'ticker', 'week_start') 