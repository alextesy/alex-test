import os
from dotenv import load_dotenv
from dump_to_bigquery import firestore_to_bigquery_etl
from firebase_functions import scheduler_fn

# Load environment variables
load_dotenv()

# Test the function
if __name__ == "__main__":
    # Create a mock event
    # Note: ScheduledEvent doesn't take a context parameter directly
    # This is a simplified version for testing
    event = scheduler_fn.ScheduledEvent()
    
    # Run the function
    result = firestore_to_bigquery_etl(event)
    print(f"Result: {result}") 