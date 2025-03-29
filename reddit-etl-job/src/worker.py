#!/usr/bin/env python3
import asyncio
import logging
import os
from datetime import timedelta
from dotenv import load_dotenv

from temporalio.client import Client
from temporalio.worker import Worker

# Import workflow and activities
from src.workflows.reddit_etl_workflow import RedditEtlWorkflow
from src.activities.extraction_activities import extract_reddit_data_activity
from src.activities.analysis_activities import analyze_stock_mentions_activity
from src.activities.aggregation_activities import (
    aggregate_daily_summaries_activity,
    aggregate_hourly_summaries_activity,
    aggregate_weekly_summaries_activity
)
from src.activities.persistence_activities import (
    save_stock_mentions_activity,
    save_daily_summaries_activity,
    save_hourly_summaries_activity,
    save_weekly_summaries_activity
)
from src.activities.state_activities import get_last_run_activity, update_run_timestamp_activity

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get Temporal server connection details
TEMPORAL_HOST = os.getenv('TEMPORAL_HOST', 'localhost:7233')
TASK_QUEUE = os.getenv('TEMPORAL_TASK_QUEUE', 'reddit-etl-task-queue')

async def main():
    """Run the Temporal worker."""
    logger.info(f"Connecting to Temporal server at {TEMPORAL_HOST}")
    client = await Client.connect(TEMPORAL_HOST)
    
    # Create a worker that hosts the workflow and activities
    logger.info(f"Starting worker on task queue: {TASK_QUEUE}")
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[RedditEtlWorkflow],
        activities=[
            extract_reddit_data_activity,
            analyze_stock_mentions_activity,
            aggregate_daily_summaries_activity,
            aggregate_hourly_summaries_activity,
            aggregate_weekly_summaries_activity,
            save_stock_mentions_activity,
            save_daily_summaries_activity,
            save_hourly_summaries_activity,
            save_weekly_summaries_activity,
            get_last_run_activity,
            update_run_timestamp_activity
        ]
    )
    
    # Start the worker (runs until shutdown)
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main()) 