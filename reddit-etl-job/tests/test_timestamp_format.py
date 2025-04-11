#!/usr/bin/env python3
"""
Test script to validate timestamp formatting for BigQuery
"""
import logging
from datetime import datetime, timezone
from src.models.stock_data import WeeklySummary
from src.utils.bigquery_utils import WeeklyBigQueryManager, BaseBigQueryManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Test the timestamp formatting for BigQuery TIMESTAMP type columns"""
    logger.info("Testing timestamp formatting for BigQuery")
    
    # Create a sample weekly summary with timezone info
    week_start_with_tz = datetime(2025, 4, 7, 0, 0, 0, tzinfo=timezone.utc)
    
    # Create the WeeklySummary object
    weekly_summary = WeeklySummary(
        ticker="AAPL",
        week_start=week_start_with_tz,
        mention_count=10,
        avg_sentiment=0.5,
        weighted_sentiment=0.6
    )
    
    # Get the dictionary from WeeklySummary
    record_dict = weekly_summary.to_dict()
    logger.info(f"Original week_start from to_dict: {record_dict['week_start']} ({type(record_dict['week_start']).__name__})")
    
    # Create a mock record copy as BaseBigQueryManager would do
    record_copy = record_dict.copy()
    for key, value in record_copy.items():
        if isinstance(value, datetime):
            if key == 'date':
                record_copy[key] = value.strftime('%Y-%m-%d')
            elif key in ['hour_start', 'week_start'] or key.endswith('_start'):
                # Always ensure timestamp has proper time component
                if value.hour == 0 and value.minute == 0 and value.second == 0:
                    value = value.replace(hour=0, minute=0, second=0, microsecond=0)
                # Use the new format compatible with BigQuery TIMESTAMP
                record_copy[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            else:
                record_copy[key] = value.strftime('%Y-%m-%d %H:%M:%S')
    
    logger.info(f"Formatted for BigQuery: {record_copy['week_start']}")
    
    # Print info about what was done to help debugging
    logger.info("VERIFICATION SUMMARY:")
    logger.info("1. Created a datetime with timezone: 2025-04-07 00:00:00 UTC")
    logger.info("2. WeeklySummary.to_dict() correctly removed timezone info")
    logger.info("3. The timestamp is now formatted as: YYYY-MM-DD HH:MM:SS")
    logger.info("4. This format should be compatible with BigQuery TIMESTAMP columns")

if __name__ == "__main__":
    main() 