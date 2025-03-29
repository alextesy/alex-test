#!/usr/bin/env python3
"""
Reddit ETL Job for Stock Analysis

A Cloud Run job that extracts stock-related information from Reddit posts 
and performs sentiment analysis and aggregation.

This is the main entry point that executes the Temporal workflow.
"""
import logging
import os
import sys
import asyncio
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def check_environment():
    """Check if all required environment variables are set."""
    required_vars = [
        'GOOGLE_CLOUD_PROJECT_ID',
        'BIGQUERY_DATASET',
        'TEMPORAL_HOST',
        'TEMPORAL_TASK_QUEUE'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    
    return True

def main():
    """Main entry point for the Reddit ETL job."""
    logger.info("Starting Reddit ETL job for stock analysis")
    
    # Check environment variables
    if not check_environment():
        logger.error("Environment check failed, exiting")
        sys.exit(1)
    
    try:
        # Import here to avoid loading modules if environment check fails
        from src.starter import main as start_workflow
        
        # Run the workflow starter
        result = asyncio.run(start_workflow())
        
        logger.info(f"ETL job completed successfully with result: {result}")
        
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 