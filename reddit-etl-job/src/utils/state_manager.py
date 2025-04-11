import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
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
    
    def get_step_last_run_timestamp(self, step_name: str) -> Optional[datetime]:
        """
        Get the timestamp of the last successful run for a specific ETL step.
        
        Args:
            step_name: Name of the ETL step
            
        Returns:
            datetime or None: The timestamp of the last run for the step, or None if no previous run
        """
        logger.info(f"Checking for last run timestamp for step '{step_name}'")
        
        try:
            # Get state document
            doc_ref = self.client.collection(self.collection).document(self.document)
            doc = doc_ref.get()
            
            if doc.exists:
                state_data = doc.to_dict()
                steps_data = state_data.get('steps', {})
                step_data = steps_data.get(step_name, {})
                last_run = step_data.get('last_run_timestamp')
                
                if last_run:
                    if isinstance(last_run, datetime):
                        last_timestamp = last_run
                    else:
                        # Convert string to datetime if needed
                        last_timestamp = datetime.fromisoformat(last_run.replace('Z', '+00:00'))
                    
                    logger.info(f"Found last run timestamp for step '{step_name}': {last_timestamp}")
                    return last_timestamp
        
            logger.info(f"No previous run found for step '{step_name}', will process all available data")
            return None
            
        except Exception as e:
            logger.error(f"Error getting last run timestamp for step '{step_name}': {str(e)}", exc_info=True)
            # Return None to process all data if state retrieval fails
            logger.info(f"Error retrieving last run timestamp for step '{step_name}', will process all available data")
            return None
    
    def get_all_step_timestamps(self) -> Dict[str, Any]:
        """
        Get all ETL step timestamps.
        
        Returns:
            Dictionary with timestamps for all ETL steps
        """
        logger.info("Getting all ETL step timestamps")
        
        try:
            # Get state document
            doc_ref = self.client.collection(self.collection).document(self.document)
            doc = doc_ref.get()
            
            if doc.exists:
                state_data = doc.to_dict()
                steps_data = state_data.get('steps', {})
                return steps_data
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting all step timestamps: {str(e)}", exc_info=True)
            return {}
    
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
    
    def update_step_timestamp(self, step_name: str, timestamp: Optional[datetime] = None):
        """
        Update the timestamp of the last successful run for a specific ETL step.
        
        Args:
            step_name: Name of the ETL step
            timestamp: The timestamp to save (defaults to current UTC time)
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
            
        logger.info(f"Updating timestamp for step '{step_name}' to: {timestamp}")
        
        try:
            # Update or create state document
            doc_ref = self.client.collection(self.collection).document(self.document)
            
            # Create or update step data
            doc_ref.set({
                'steps': {
                    step_name: {
                        'last_run_timestamp': timestamp,
                        'updated_at': datetime.utcnow()
                    }
                },
                'updated_at': datetime.utcnow()
            }, merge=True)
            
            logger.info(f"Successfully updated timestamp for step '{step_name}'")
            
        except Exception as e:
            logger.error(f"Error updating timestamp for step '{step_name}': {str(e)}", exc_info=True)
            # Continue execution even if state update fails 