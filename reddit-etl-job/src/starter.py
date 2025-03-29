#!/usr/bin/env python3
import asyncio
import logging
import os
from datetime import timedelta
import uuid
from dotenv import load_dotenv

from temporalio.client import Client

# Import workflow
from src.workflows.reddit_etl_workflow import RedditEtlWorkflow

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
    """Start a new Reddit ETL workflow execution."""
    logger.info(f"Connecting to Temporal server at {TEMPORAL_HOST}")
    client = await Client.connect(TEMPORAL_HOST)
    
    # Generate a unique workflow ID
    workflow_id = f"reddit-etl-{uuid.uuid4()}"
    
    logger.info(f"Starting Reddit ETL workflow with ID: {workflow_id}")
    
    # Start the workflow execution
    handle = await client.start_workflow(
        RedditEtlWorkflow.run,
        id=workflow_id,
        task_queue=TASK_QUEUE,
        execution_timeout=timedelta(hours=2)
    )
    
    logger.info(f"Workflow started with ID: {workflow_id}")
    
    # Wait for the workflow to complete and get the result
    result = await handle.result()
    logger.info(f"Workflow completed with result: {result}")
    
    return result

if __name__ == "__main__":
    asyncio.run(main()) 