import os
import logging
import json
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, Text, JSON
from datetime import datetime
from typing import Dict, Any, List, Optional, TypeVar, Generic, Type

logger = logging.getLogger(__name__)

# Define type variables for generic types
T = TypeVar('T')

class DBManager:
    """
    Database manager class for PostgreSQL operations.
    """
    
    _instance = None
    
    def __new__(cls):
        """Singleton pattern to ensure only one instance exists."""
        if cls._instance is None:
            cls._instance = super(DBManager, cls).__new__(cls)
            cls._instance.initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the database manager."""
        if not getattr(self, 'initialized', False):
            self.db_host = os.getenv('DB_HOST')
            self.db_port = os.getenv('DB_PORT', '5432')
            self.db_name = os.getenv('DB_NAME')
            self.db_user = os.getenv('DB_USER')
            self.db_password = os.getenv('DB_PASSWORD')
            self.engine = None
            self.metadata = None
            self.tables = {}
            self.initialized = True
        
    def connect(self) -> sqlalchemy.engine.Engine:
        """
        Create a database connection.
        
        Returns:
            SQLAlchemy engine
        """
        if self.engine is None:
            # Create PostgreSQL connection string
            db_uri = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
            self.engine = create_engine(db_uri)
            logger.info(f"Connected to PostgreSQL database at {self.db_host}")
            
        return self.engine
    
    def setup_tables(self):
        """
        Set up PostgreSQL tables for stock data.
        """
        logger.info("Setting up PostgreSQL tables for stock data")
        
        engine = self.connect()
        self.metadata = MetaData()
        
        # Create stock_mentions table
        self.tables['stock_mentions'] = Table(
            'stock_mentions',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('message_id', String(100)),
            Column('ticker', String(10)),
            Column('author', String(100)),
            Column('created_at', DateTime),
            Column('subreddit', String(100)),
            Column('url', String(500)),
            Column('score', Integer),
            Column('message_type', String(20)),
            Column('sentiment_compound', Float),
            Column('sentiment_positive', Float),
            Column('sentiment_negative', Float),
            Column('sentiment_neutral', Float),
            Column('signals', JSON),
            Column('context', Text),
            Column('confidence', Float),
            Column('etl_timestamp', DateTime, default=datetime.utcnow)
        )
        
        # Create stock_daily_summary table
        self.tables['stock_daily_summary'] = Table(
            'stock_daily_summary',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('ticker', String(10)),
            Column('date', DateTime),
            Column('mention_count', Integer),
            Column('avg_sentiment', Float),
            Column('weighted_sentiment', Float),
            Column('buy_signals', Integer),
            Column('sell_signals', Integer),
            Column('hold_signals', Integer),
            Column('price_targets', JSON),
            Column('news_signals', Integer),
            Column('earnings_signals', Integer),
            Column('technical_signals', Integer),
            Column('options_signals', Integer),
            Column('avg_confidence', Float),
            Column('high_conf_sentiment', Float),
            Column('top_contexts', JSON),
            Column('subreddits', JSON),
            Column('etl_timestamp', DateTime, default=datetime.utcnow)
        )
        
        # Create stock_hourly_summary table
        self.tables['stock_hourly_summary'] = Table(
            'stock_hourly_summary',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('ticker', String(10)),
            Column('hour_start', DateTime),
            Column('mention_count', Integer),
            Column('avg_sentiment', Float),
            Column('weighted_sentiment', Float),
            Column('buy_signals', Integer),
            Column('sell_signals', Integer),
            Column('hold_signals', Integer),
            Column('avg_confidence', Float),
            Column('subreddits', JSON),
            Column('etl_timestamp', DateTime, default=datetime.utcnow)
        )
        
        # Create stock_weekly_summary table
        self.tables['stock_weekly_summary'] = Table(
            'stock_weekly_summary',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('ticker', String(10)),
            Column('week_start', DateTime),
            Column('mention_count', Integer),
            Column('avg_sentiment', Float),
            Column('weighted_sentiment', Float),
            Column('buy_signals', Integer),
            Column('sell_signals', Integer),
            Column('hold_signals', Integer),
            Column('price_targets', JSON),
            Column('news_signals', Integer),
            Column('earnings_signals', Integer),
            Column('technical_signals', Integer),
            Column('options_signals', Integer),
            Column('avg_confidence', Float),
            Column('daily_breakdown', JSON),
            Column('subreddits', JSON),
            Column('etl_timestamp', DateTime, default=datetime.utcnow)
        )
        
        # Create tables if they don't exist
        self.metadata.create_all(engine)
        logger.info("PostgreSQL tables created or updated")
    
    def check_stock_mention_exists(self, message_id: str, ticker: str) -> bool:
        """
        Check if a stock mention already exists in the database.
        
        Args:
            message_id: ID of the message
            ticker: Stock ticker
            
        Returns:
            bool: True if the mention exists, False otherwise
        """
        engine = self.connect()
        
        with engine.connect() as connection:
            query = sqlalchemy.text("""
            SELECT COUNT(*) FROM stock_mentions 
            WHERE message_id = :message_id AND ticker = :ticker
            """)
            
            result = connection.execute(query, {'message_id': message_id, 'ticker': ticker}).scalar()
            
        return result > 0
    
    def insert_stock_mention(self, mention_data: Dict[str, Any]):
        """
        Insert a stock mention into the database.
        
        Args:
            mention_data: Dictionary with stock mention data
        """
        engine = self.connect()
        
        with engine.connect() as connection:
            # Check if it already exists
            if self.check_stock_mention_exists(mention_data['message_id'], mention_data['ticker']):
                logger.debug(f"Stock mention {mention_data['message_id']} - {mention_data['ticker']} already exists, skipping")
                return
                
            # Insert new mention
            query = sqlalchemy.text("""
            INSERT INTO stock_mentions 
            (message_id, ticker, author, created_at, subreddit, url, score, 
             message_type, sentiment_compound, sentiment_positive, sentiment_negative, 
             sentiment_neutral, signals, context, confidence, etl_timestamp) 
            VALUES (:message_id, :ticker, :author, :created_at, :subreddit, :url, :score, 
                    :message_type, :sentiment_compound, :sentiment_positive, :sentiment_negative,
                    :sentiment_neutral, :signals, :context, :confidence, :etl_timestamp)
            """)
            
            connection.execute(query, mention_data)
    
    def bulk_insert_stock_mentions(self, mentions: List[Dict[str, Any]]):
        """
        Bulk insert stock mentions, skipping any that already exist.
        
        Args:
            mentions: List of stock mention dictionaries
        """
        if not mentions:
            return
            
        engine = self.connect()
        
        with engine.connect() as connection:
            # Check for duplicates first
            message_tickers = [(m['message_id'], m['ticker']) for m in mentions]
            
            # Prepare placeholders for SQL query
            placeholders = ', '.join([f"('{m_id}', '{ticker}')" for m_id, ticker in message_tickers])
            
            # Find existing mentions
            query = f"""
            SELECT message_id, ticker FROM stock_mentions 
            WHERE (message_id, ticker) IN ({placeholders})
            """
            
            existing_mentions = connection.execute(sqlalchemy.text(query)).fetchall()
            existing_pairs = {(row[0], row[1]) for row in existing_mentions}
            
            # Filter out duplicates
            new_mentions = [
                m for m in mentions 
                if (m['message_id'], m['ticker']) not in existing_pairs
            ]
            
            if existing_pairs:
                logger.info(f"Skipping {len(existing_pairs)} already existing stock mentions")
            
            # Insert only new mentions
            with connection.begin():
                for mention in new_mentions:
                    connection.execute(
                        sqlalchemy.text("""
                        INSERT INTO stock_mentions 
                        (message_id, ticker, author, created_at, subreddit, url, score, 
                         message_type, sentiment_compound, sentiment_positive, sentiment_negative, 
                         sentiment_neutral, signals, context, confidence, etl_timestamp) 
                        VALUES (:message_id, :ticker, :author, :created_at, :subreddit, :url, :score, 
                                :message_type, :sentiment_compound, :sentiment_positive, :sentiment_negative,
                                :sentiment_neutral, :signals, :context, :confidence, :etl_timestamp)
                        """),
                        mention
                    )
            
            logger.info(f"Inserted {len(new_mentions)} new stock mentions")


class BaseDataManager(Generic[T]):
    """
    Base data manager for handling common database operations
    on different types of aggregations.
    """
    
    def __init__(self, table_name: str, id_field: str, ticker_field: str, date_field: str):
        """
        Initialize base data manager.
        
        Args:
            table_name: Name of the database table
            id_field: Name of the ID field
            ticker_field: Name of the ticker field
            date_field: Name of the date/time field
        """
        self.db_manager = DBManager()
        self.table_name = table_name
        self.id_field = id_field
        self.ticker_field = ticker_field
        self.date_field = date_field
    
    def get_existing_record(self, ticker: str, date_value: str) -> Optional[Dict[str, Any]]:
        """
        Get an existing record from the database.
        
        Args:
            ticker: The stock ticker
            date_value: The date/time string
            
        Returns:
            Dict containing the record if found, None otherwise
        """
        engine = self.db_manager.connect()
        
        with engine.connect() as connection:
            query = f"""
            SELECT * FROM {self.table_name}
            WHERE {self.ticker_field} = :ticker AND {self.date_field}::date = :date_value::date
            """
            
            result = connection.execute(
                sqlalchemy.text(query),
                {'ticker': ticker, 'date_value': date_value}
            ).fetchone()
            
            if result:
                return dict(result._mapping)
            
        return None
    
    def update_record(self, record_id: int, data: Dict[str, Any]):
        """
        Update an existing record in the database.
        
        Args:
            record_id: The ID of the record to update
            data: Dictionary of data to update
        """
        engine = self.db_manager.connect()
        
        # Remove fields that shouldn't be updated
        update_data = data.copy()
        if self.id_field in update_data:
            del update_data[self.id_field]
        
        # Build SET clause for SQL
        set_clause = ", ".join([f"{key} = :{key}" for key in update_data.keys()])
        
        # Add ID to parameters
        update_data[self.id_field] = record_id
        
        with engine.connect() as connection:
            with connection.begin():
                query = f"""
                UPDATE {self.table_name}
                SET {set_clause}
                WHERE {self.id_field} = :{self.id_field}
                """
                
                connection.execute(sqlalchemy.text(query), update_data)
                
        logger.debug(f"Updated {self.table_name} record with ID {record_id}")
    
    def insert_record(self, data: Dict[str, Any]) -> int:
        """
        Insert a new record into the database.
        
        Args:
            data: Dictionary of data to insert
            
        Returns:
            ID of the newly inserted record
        """
        engine = self.db_manager.connect()
        
        # Build column and value lists
        columns = ", ".join(data.keys())
        value_placeholders = ", ".join([f":{key}" for key in data.keys()])
        
        with engine.connect() as connection:
            with connection.begin():
                query = f"""
                INSERT INTO {self.table_name}
                ({columns})
                VALUES ({value_placeholders})
                RETURNING {self.id_field}
                """
                
                result = connection.execute(sqlalchemy.text(query), data)
                record_id = result.scalar()
                
        logger.debug(f"Inserted new {self.table_name} record with ID {record_id}")
        return record_id
    
    def bulk_insert_records(self, records: List[Dict[str, Any]]) -> int:
        """
        Bulk insert records into the database.
        
        Args:
            records: List of record dictionaries
            
        Returns:
            Number of records inserted
        """
        if not records:
            return 0
            
        engine = self.db_manager.connect()
        
        with engine.connect() as connection:
            with connection.begin():
                for record in records:
                    # Build column and value lists
                    columns = ", ".join(record.keys())
                    value_placeholders = ", ".join([f":{key}" for key in record.keys()])
                    
                    query = f"""
                    INSERT INTO {self.table_name}
                    ({columns})
                    VALUES ({value_placeholders})
                    """
                    
                    connection.execute(sqlalchemy.text(query), record)
                
        logger.info(f"Inserted {len(records)} new records into {self.table_name}")
        return len(records)
    
    def save_records(self, records: List[T]) -> int:
        """
        Save a list of model objects to the database,
        checking for existing records to update.
        
        Args:
            records: List of model objects
            
        Returns:
            Number of records saved (inserted or updated)
        """
        if not records:
            return 0
            
        saved_count = 0
        for record in records:
            record_dict = record.to_dict()
            
            # Determine if it's an update or insert
            if hasattr(record, 'id') and getattr(record, 'id'):
                self.update_record(getattr(record, 'id'), record_dict)
            else:
                date_field_value = getattr(record, self.date_field.replace('_start', ''))
                date_value = date_field_value.strftime('%Y-%m-%d')
                
                # Check if record exists
                existing = self.get_existing_record(record.ticker, date_value)
                if existing:
                    self.update_record(existing[self.id_field], record_dict)
                else:
                    self.insert_record(record_dict)
                    
            saved_count += 1
                
        return saved_count


# Create specialized managers
class DailySummaryManager(BaseDataManager):
    """Data manager for daily summaries."""
    
    def __init__(self):
        super().__init__('stock_daily_summary', 'id', 'ticker', 'date')


class HourlySummaryManager(BaseDataManager):
    """Data manager for hourly summaries."""
    
    def __init__(self):
        super().__init__('stock_hourly_summary', 'id', 'ticker', 'hour_start')


class WeeklySummaryManager(BaseDataManager):
    """Data manager for weekly summaries."""
    
    def __init__(self):
        super().__init__('stock_weekly_summary', 'id', 'ticker', 'week_start') 