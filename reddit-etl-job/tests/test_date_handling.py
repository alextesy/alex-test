#!/usr/bin/env python3
from datetime import datetime
from src.models.stock_data import WeeklySummary
from src.utils.bigquery_utils import BaseBigQueryManager
import json

# Create a test weekly summary
week_start = datetime(2025, 4, 7)
ws = WeeklySummary(
    ticker='AAPL', 
    week_start=week_start,
    mention_count=10, 
    avg_sentiment=0.5, 
    weighted_sentiment=0.6
)

# Print the original datetime object
print(f"Original week_start: {week_start}")

# Get the dictionary representation from the model
record_dict = ws.to_dict()
print(f"From to_dict(): week_start ({type(record_dict['week_start']).__name__}): {record_dict['week_start']}")

# Create a mock record copy and process it as BaseBigQueryManager would
record_copy = record_dict.copy()
for key, value in record_copy.items():
    if isinstance(value, datetime):
        if key == 'week_start':
            # Original formatting logic
            if value.hour == 0 and value.minute == 0 and value.second == 0:
                value = value.replace(hour=0, minute=0, second=0, microsecond=0)
            # This is what we're currently doing:
            current_format = value.isoformat()
            # Let's try alternative formatting without timezone:
            alternative_format = value.strftime('%Y-%m-%dT%H:%M:%S')
            
            print(f"Current format (isoformat): {current_format}")
            print(f"Alternative format (strftime): {alternative_format}")
            
            # Use the alternative format to fix the issue
            record_copy[key] = alternative_format

# Print the JSON representation as it would be sent to BigQuery
print("\nJSON representation:")
print(json.dumps(record_copy, default=str, indent=2)) 