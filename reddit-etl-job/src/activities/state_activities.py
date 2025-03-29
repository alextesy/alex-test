import logging
from datetime import datetime
from typing import Optional

from temporalio import activity

from src.utils.state_manager import StateManager

logger = logging.getLogger(__name__)

@activity.defn
async def get_last_run_activity() -> Optional[datetime]:
    """
    Activity to get the timestamp of the last successful ETL run.
    
    Returns:
        datetime or None: The timestamp of the last run, or None if no previous run
    """
    logger.info("Starting state activity: Getting last ETL run timestamp")
    
    state_manager = StateManager()
    last_run_time = state_manager.get_last_run_timestamp()
    
    if last_run_time:
        logger.info(f"Found last ETL run timestamp: {last_run_time}")
    else:
        logger.info("No previous ETL run found, will process all available data")
    
    return last_run_time

@activity.defn
async def update_run_timestamp_activity(timestamp: Optional[datetime] = None) -> datetime:
    """
    Activity to update the timestamp of the last successful ETL run.
    
    Args:
        timestamp: The timestamp to save (defaults to current UTC time)
        
    Returns:
        datetime: The saved timestamp
    """
    if timestamp is None:
        timestamp = datetime.utcnow()
        
    logger.info(f"Starting state activity: Updating ETL run timestamp to {timestamp}")
    
    state_manager = StateManager()
    state_manager.update_run_timestamp(timestamp)
    
    logger.info("Successfully updated ETL run timestamp")
    
    return timestamp 