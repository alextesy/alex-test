import logging
import asyncio
import json
from datetime import timedelta
import uuid
from typing import Dict, Any
from flask import Flask, request, jsonify, Response

from temporalio.client import Client

from src.workflows.reddit_etl_workflow import RedditEtlWorkflow

# Configure logging
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Global client for Temporal
temporal_client = None

async def init_temporal():
    """Initialize the Temporal client."""
    import os
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Get Temporal server connection details
    TEMPORAL_HOST = os.getenv('TEMPORAL_HOST', 'localhost:7233')
    
    global temporal_client
    if temporal_client is None:
        try:
            temporal_client = await Client.connect(TEMPORAL_HOST)
            logger.info(f"Connected to Temporal server at {TEMPORAL_HOST}")
        except Exception as e:
            logger.error(f"Failed to connect to Temporal server: {str(e)}")
            raise

async def start_workflow() -> Dict[str, Any]:
    """Start the Reddit ETL workflow."""
    import os
    
    # Get Temporal task queue
    TEMPORAL_TASK_QUEUE = os.getenv('TEMPORAL_TASK_QUEUE', 'reddit-etl-task-queue')
    
    await init_temporal()
    
    # Generate a unique workflow ID
    workflow_id = f"reddit-etl-{uuid.uuid4()}"
    
    logger.info(f"Starting Reddit ETL workflow with ID: {workflow_id}")
    
    # Start the workflow execution asynchronously (don't wait for completion)
    handle = await temporal_client.start_workflow(
        RedditEtlWorkflow.run,
        id=workflow_id,
        task_queue=TEMPORAL_TASK_QUEUE,
        execution_timeout=timedelta(hours=2)
    )
    
    return {
        "status": "started",
        "workflow_id": workflow_id,
        "message": "Workflow started successfully"
    }

@app.route('/health', methods=['GET'])
def health_check() -> Response:
    """Health check endpoint."""
    return jsonify({"status": "healthy"})

@app.route('/start', methods=['POST'])
def start_etl() -> Response:
    """Start the ETL workflow."""
    try:
        result = asyncio.run(start_workflow())
        return jsonify(result)
    except Exception as e:
        logger.error(f"Failed to start workflow: {str(e)}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/status/<workflow_id>', methods=['GET'])
def workflow_status(workflow_id: str) -> Response:
    """Get the status of a workflow."""
    async def get_status():
        await init_temporal()
        
        try:
            handle = temporal_client.get_workflow_handle(workflow_id)
            desc = await handle.describe()
            
            status = {
                "workflow_id": workflow_id,
                "run_id": desc.run_id,
                "status": desc.status.name,
                "start_time": desc.start_time.isoformat() if desc.start_time else None,
                "close_time": desc.close_time.isoformat() if desc.close_time else None
            }
            
            return status
        except Exception as e:
            logger.error(f"Failed to get workflow status: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    try:
        result = asyncio.run(get_status())
        return jsonify(result)
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500 