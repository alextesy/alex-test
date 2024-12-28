from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker, declarative_base
import logging
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

logger = logging.getLogger(__name__)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def init_db():
    """Initialize the database by creating all tables."""
    try:
        logger.info("Checking database tables...")
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        
        # Get all tables that should exist from our models
        metadata_tables = Base.metadata.tables.keys()
        
        # Log status of each table
        for table_name in metadata_tables:
            if table_name in existing_tables:
                logger.info(f"Table '{table_name}' already exists")
            else:
                logger.info(f"Table '{table_name}' needs to be created")
        
        logger.info("Creating missing tables...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database initialization completed")
        
    except Exception as e:
        logger.error(f"Error during database initialization: {str(e)}")
        raise

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close() 