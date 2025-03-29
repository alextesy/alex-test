import os
import logging
from datetime import datetime, timedelta
from typing import Optional
from google.cloud import firestore

logger = logging.getLogger(__name__)

class StateManager:
    """
    Manages the state of ETL runs using Firestore.
    """
    
    def __init__(self, collection: str = 'etl_state', document: str = 'stock_etl_state'):
        """
        Initialize the state manager.
        
        Args:
            collection: Firestore collection name
            document: Firestore document ID
        """
        self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT_ID')
        self.collection = collection
        self.document = document
        self._client = None
    
    @property
    def client(self) -> firestore.Client:
        """
        Lazy-loaded Firestore client.
        
        Returns:
            Firestore client
        """
        if self._client is None:
            self._client = firestore.Client(project=self.project_id)
        return self._client
    
    def get_last_run_timestamp(self) -> Optional[datetime]:
        """
        Get the timestamp of the last successful ETL run.
        
        Returns:
            datetime or None: The timestamp of the last run, or None if no previous run
        """
        logger.info("Checking for last ETL run timestamp")
        
        try:
            # Get state document
            doc_ref = self.client.collection(self.collection).document(self.document)
            doc = doc_ref.get()
            
            if doc.exists:
                state_data = doc.to_dict()
                last_run = state_data.get('last_run_timestamp')
                
                if last_run:
                    if isinstance(last_run, datetime):
                        last_timestamp = last_run
                    else:
                        # Convert string to datetime if needed
                        last_timestamp = datetime.fromisoformat(last_run.replace('Z', '+00:00'))
                    
                    logger.info(f"Found last ETL run timestamp: {last_timestamp}")
                    return last_timestamp
        
            logger.info("No previous ETL run found, will process all available data")
            return None
            
        except Exception as e:
            logger.error(f"Error getting last run timestamp: {str(e)}", exc_info=True)
            # Return None to process all data if state retrieval fails
            logger.info("Error retrieving last run timestamp, will process all available data")
            return None
    
    def update_run_timestamp(self, timestamp: Optional[datetime] = None):
        """
        Update the timestamp of the last successful ETL run.
        
        Args:
            timestamp: The timestamp to save (defaults to current UTC time)
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
            
        logger.info(f"Updating ETL run timestamp to: {timestamp}")
        
        try:
            # Update or create state document
            doc_ref = self.client.collection(self.collection).document(self.document)
            doc_ref.set({
                'last_run_timestamp': timestamp,
                'updated_at': datetime.utcnow()
            }, merge=True)
            
            logger.info("Successfully updated ETL run timestamp")
            
        except Exception as e:
            logger.error(f"Error updating run timestamp: {str(e)}", exc_info=True)
            # Continue execution even if state update fails 