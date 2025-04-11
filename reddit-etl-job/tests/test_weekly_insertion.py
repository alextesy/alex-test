#!/usr/bin/env python3
"""
Test script for weekly summary insertion
This script creates a sample weekly summary and simulates the process
of preparing it for BigQuery insertion with our new fix.
"""
import os
import logging
from datetime import datetime, timezone
from src.models.stock_data import WeeklySummary, HourlySummary, DailySummary
from src.utils.bigquery_utils import WeeklyBigQueryManager, BaseBigQueryManager
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_weekly_summary():
    logger.info("=== Testing WeeklySummary ===")
    # Create a weekly summary with an explicit timezone to test the fix
    # This simulates what might be happening in production
    week_start_with_tz = datetime(2025, 4, 7, 0, 0, 0, tzinfo=timezone.utc)
    logger.info(f"Created week_start with timezone: {week_start_with_tz} (has tzinfo: {week_start_with_tz.tzinfo is not None})")
    
    # Create the weekly summary object
    weekly_summary = WeeklySummary(
        ticker="AAPL",
        week_start=week_start_with_tz,
        mention_count=25,
        avg_sentiment=0.75,
        weighted_sentiment=0.8
    )
    
    # Convert to dictionary as would happen in the pipeline
    summary_dict = weekly_summary.to_dict()
    logger.info(f"WeeklySummary.to_dict() produced: {summary_dict['week_start']} (type: {type(summary_dict['week_start']).__name__}, has tzinfo: {getattr(summary_dict['week_start'], 'tzinfo', None) is not None})")
    
    # Simulate how BaseBigQueryManager.insert_or_update_records would process this
    record_copy = summary_dict.copy()
    for key, value in record_copy.items():
        if isinstance(value, datetime):
            if key == 'date':
                record_copy[key] = value.strftime('%Y-%m-%d')
            elif key in ['hour_start', 'week_start'] or key.endswith('_start'):
                if value.hour == 0 and value.minute == 0 and value.second == 0:
                    value = value.replace(hour=0, minute=0, second=0, microsecond=0)
                record_copy[key] = value.isoformat()
            else:
                record_copy[key] = value.isoformat()
    
    logger.info(f"Final timestamp format for BigQuery: {record_copy['week_start']}")

def test_hourly_summary():
    logger.info("=== Testing HourlySummary ===")
    # Create an hourly summary with timezone info
    hour_start_with_tz = datetime(2025, 4, 7, 14, 0, 0, tzinfo=timezone.utc)
    logger.info(f"Created hour_start with timezone: {hour_start_with_tz} (has tzinfo: {hour_start_with_tz.tzinfo is not None})")
    
    hourly_summary = HourlySummary(
        ticker="MSFT",
        hour_start=hour_start_with_tz,
        mention_count=12,
        avg_sentiment=0.65,
        weighted_sentiment=0.7
    )
    
    # Convert to dictionary
    summary_dict = hourly_summary.to_dict()
    logger.info(f"HourlySummary.to_dict() produced: {summary_dict['hour_start']} (type: {type(summary_dict['hour_start']).__name__}, has tzinfo: {getattr(summary_dict['hour_start'], 'tzinfo', None) is not None})")

def test_daily_summary():
    logger.info("=== Testing DailySummary ===")
    # Create a daily summary with timezone info
    date_with_tz = datetime(2025, 4, 7, 0, 0, 0, tzinfo=timezone.utc)
    logger.info(f"Created date with timezone: {date_with_tz} (has tzinfo: {date_with_tz.tzinfo is not None})")
    
    daily_summary = DailySummary(
        ticker="GOOG",
        date=date_with_tz,
        mention_count=30,
        avg_sentiment=0.8,
        weighted_sentiment=0.85
    )
    
    # Convert to dictionary
    summary_dict = daily_summary.to_dict()
    logger.info(f"DailySummary.to_dict() produced: {summary_dict['date']} (type: {type(summary_dict['date']).__name__}, has tzinfo: {getattr(summary_dict['date'], 'tzinfo', None) is not None})")

def main():
    logger.info("Starting timestamp handling test")
    
    test_weekly_summary()
    test_hourly_summary()
    test_daily_summary()
    
    logger.info("Test completed successfully")

if __name__ == "__main__":
    main() 