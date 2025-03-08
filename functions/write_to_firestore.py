import functions_framework
from google.cloud import firestore
from datetime import datetime
import json

@functions_framework.http
def write_stock_data(request):
    """HTTP Cloud Function to write stock data to Firestore.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    try:
        request_json = request.get_json(silent=True)
        
        if not request_json:
            return 'No data provided', 400
            
        # Initialize Firestore client
        db = firestore.Client()
        
        # Create a new document with current timestamp
        timestamp = datetime.utcnow()
        collection_name = 'raw_stock_data'
        
        # Add timestamp to the data
        data = {
            'timestamp': timestamp,
            'data': request_json,
            'source': request_json.get('source', 'unknown')
        }
        
        # Write to Firestore
        doc_ref = db.collection(collection_name).document()
        doc_ref.set(data)
        
        return json.dumps({
            'status': 'success',
            'message': 'Data written to Firestore',
            'document_id': doc_ref.id
        }), 200
        
    except Exception as e:
        return json.dumps({
            'status': 'error',
            'message': str(e)
        }), 500 