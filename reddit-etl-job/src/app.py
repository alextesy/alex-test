#!/usr/bin/env python3
"""
Flask application for the Reddit ETL API.
This provides HTTP endpoints for starting and monitoring ETL workflows.
"""
import os
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Import the Flask routes
from src.routes import app

if __name__ == "__main__":
    PORT = int(os.getenv("PORT", 8080))
    logger.info(f"Starting Flask server on port {PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False) 