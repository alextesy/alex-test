import os
import logging
from typing import List, Dict, Any, TypeVar, Generic, Optional, Type
from datetime import datetime

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

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
        
        # Filter out records that already exist in BigQuery
        # (In a production environment, consider batching this check for better performance)
        new_mentions = []
        for mention in mentions:
            if not self.check_stock_mention_exists(mention['message_id'], mention['ticker']):
                # Convert signals to string if it's not already
                if 'signals' in mention and mention['signals'] is not None:
                    if not isinstance(mention['signals'], str):
                        mention['signals'] = str(mention['signals'])
                new_mentions.append(mention)
        
        if not new_mentions:
            logger.info("No new stock mentions to insert")
            return
        
        # Insert records into BigQuery
        errors = client.insert_rows_json(table_id, new_mentions)
        
        if errors:
            logger.error(f"Errors inserting stock mentions: {errors}")
        else:
            logger.info(f"Successfully inserted {len(new_mentions)} stock mentions to BigQuery")


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
    
    def insert_or_update_records(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert or update records in BigQuery.
        
        Args:
            records: List of record dictionaries
        
        Returns:
            int: Number of records inserted or updated
        """
        if not records:
            return 0
        
        table_id = f"{self.project_id}.{self.dataset_id}.{self.table_name}"
        
        # Process records for BigQuery compatibility
        processed_records = []
        for record in records:
            # Convert any complex types to JSON strings
            for key, value in record.items():
                if isinstance(value, (dict, list)):
                    record[key] = str(value)
            processed_records.append(record)
        
        # Use BigQuery merge operation
        merge_query = f"""
        MERGE `{self.project_id}.{self.dataset_id}.{self.table_name}` T
        USING (
            {self._create_values_subquery(processed_records)}
        ) S
        ON T.{self.ticker_field} = S.{self.ticker_field} AND T.{self.date_field} = S.{self.date_field}
        WHEN MATCHED THEN
            UPDATE SET {self._create_update_clause(processed_records[0])}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(processed_records[0].keys())})
            VALUES ({', '.join([f'S.{k}' for k in processed_records[0].keys()])})
        """
        
        query_job = self.client.query(merge_query)
        query_job.result()
        
        return len(processed_records)
    
    def _create_values_subquery(self, records: List[Dict[str, Any]]) -> str:
        """
        Create a VALUES subquery for BigQuery MERGE operation.
        
        Args:
            records: List of record dictionaries
            
        Returns:
            str: SQL VALUES subquery
        """
        columns = records[0].keys()
        values_rows = []
        
        for record in records:
            values = []
            for col in columns:
                value = record.get(col)
                if value is None:
                    values.append("NULL")
                elif isinstance(value, str):
                    values.append("'{}'".format(value.replace("'", "\\'")))
                elif isinstance(value, bool):
                    values.append("TRUE" if value else "FALSE")
                elif isinstance(value, (int, float)):
                    values.append(str(value))
                else:
                    values.append("'{}'".format(str(value).replace("'", "\\'")))
            
            values_rows.append(f"({', '.join(values)})")
        
        return f"SELECT * FROM UNNEST([{', '.join(columns)}]) WITH ORDINALITY cols(name, pos) PIVOT (ANY_VALUE({', '.join(values_rows)}[OFFSET(pos-1)]) FOR pos IN {', '.join([str(i+1) for i in range(len(columns))])})"
    
    def _create_update_clause(self, record: Dict[str, Any]) -> str:
        """
        Create the UPDATE SET clause for BigQuery MERGE operation.
        
        Args:
            record: Sample record to extract column names
            
        Returns:
            str: SQL UPDATE SET clause
        """
        # Exclude the key fields from the update
        update_columns = [col for col in record.keys() if col not in [self.ticker_field, self.date_field]]
        return ", ".join([f"{col} = S.{col}" for col in update_columns])
    
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