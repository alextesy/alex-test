#!/usr/bin/env python3
import os
import logging
from google.cloud import firestore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def reset_etl_state():
    """Delete the ETL state document in Firestore to force a full reprocessing."""
    
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT_ID')
    if not project_id:
        logger.error("GOOGLE_CLOUD_PROJECT_ID environment variable not set!")
        return False
    
    collection = 'etl_state'
    document = 'stock_etl_state'
    
    logger.info(f"Connecting to Firestore in project {project_id}")
    client = firestore.Client(project=project_id)
    
    logger.info(f"Attempting to delete state document: {collection}/{document}")
    doc_ref = client.collection(collection).document(document)
    
    # Check if document exists
    doc = doc_ref.get()
    if doc.exists:
        # Document exists, delete it
        doc_ref.delete()
        logger.info(f"Successfully deleted ETL state document. The next run will process all data.")
        return True
    else:
        logger.info(f"ETL state document does not exist. No action needed.")
        return True

if __name__ == "__main__":
    success = reset_etl_state()
    if success:
        logger.info("ETL state reset complete. The next run will process all data.")
    else:
        logger.error("Failed to reset ETL state.") 