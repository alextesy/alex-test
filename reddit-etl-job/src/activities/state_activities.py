import logging
from datetime import datetime
from typing import Optional, Dict, Any

from temporalio import activity

from src.utils.state_manager import StateManager

logger = logging.getLogger(__name__)

# Define ETL step names as constants
STEP_EXTRACTION = "extraction"
STEP_ANALYSIS = "analysis" 
STEP_STOCK_PERSISTENCE = "stock_persistence"
STEP_DAILY_AGGREGATION = "daily_aggregation"
STEP_HOURLY_AGGREGATION = "hourly_aggregation"
STEP_WEEKLY_AGGREGATION = "weekly_aggregation"
STEP_DAILY_PERSISTENCE = "daily_persistence"
STEP_HOURLY_PERSISTENCE = "hourly_persistence"
STEP_WEEKLY_PERSISTENCE = "weekly_persistence"

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
async def get_step_last_run_activity(step_name: str) -> Optional[datetime]:
    """
    Activity to get the timestamp of the last successful run for a specific ETL step.
    
    Args:
        step_name: Name of the ETL step
        
    Returns:
        datetime or None: The timestamp of the last run for the step, or None if no previous run
    """
    logger.info(f"Starting state activity: Getting last run timestamp for step '{step_name}'")
    
    state_manager = StateManager()
    last_run_time = state_manager.get_step_last_run_timestamp(step_name)
    
    if last_run_time:
        logger.info(f"Found last run timestamp for step '{step_name}': {last_run_time}")
    else:
        logger.info(f"No previous run found for step '{step_name}', will process all available data")
    
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

@activity.defn
async def update_step_timestamp_activity(step_name: str, timestamp: Optional[datetime] = None) -> datetime:
    """
    Activity to update the timestamp of the last successful run for a specific ETL step.
    
    Args:
        step_name: Name of the ETL step
        timestamp: The timestamp to save (defaults to current UTC time)
        
    Returns:
        datetime: The saved timestamp
    """
    if timestamp is None:
        timestamp = datetime.utcnow()
        
    logger.info(f"Starting state activity: Updating timestamp for step '{step_name}' to {timestamp}")
    
    state_manager = StateManager()
    state_manager.update_step_timestamp(step_name, timestamp)
    
    logger.info(f"Successfully updated timestamp for step '{step_name}'")
    
    return timestamp

@activity.defn
async def get_all_step_timestamps_activity() -> Dict[str, Any]:
    """
    Activity to get all ETL step timestamps.
    
    Returns:
        Dictionary with timestamps for all ETL steps
    """
    logger.info("Starting state activity: Getting all ETL step timestamps")
    
    state_manager = StateManager()
    timestamps = state_manager.get_all_step_timestamps()
    
    logger.info(f"Retrieved timestamps for {len(timestamps)} ETL steps")
    
    return timestamps 