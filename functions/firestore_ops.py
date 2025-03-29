import os
import logging
import asyncio
from datetime import datetime
from google.cloud import firestore, bigquery
from scrapers.reddit_scraper_v2 import RedditScraper
from bigquery_ops import store_message_in_bigquery

logger = logging.getLogger(__name__)

# Get collection names from environment or use defaults
STOCK_DATA_COLLECTION = os.getenv('FIRESTORE_STOCK_DATA_COLLECTION', 'stock_data')
SCRAPER_STATE_COLLECTION = os.getenv('FIRESTORE_SCRAPER_STATE_COLLECTION', 'scraper_state')
PROJECT_ID = os.getenv('PROJECT_ID')
DATASET_ID = os.getenv('BIGQUERY_DATASET_ID', 'reddit_data')
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.raw_messages"


async def store_message_in_firestore(message, db):
    """
    Store or update a message in Firestore.
    This helper converts the message object to a dictionary and updates Firestore
    using merge=True, so that existing documents are updated.
    """
    try:
        data = {
            'id': message.id,
            'content': message.content,
            'author': message.author,
            'timestamp': message.timestamp,
            'url': message.url,
            'score': message.score,
            'created_at': message.created_at,
            'message_type': message.message_type,
            'source': 'reddit'
        }
        # Additional fields for Reddit posts/comments
        if hasattr(message, 'title'):
            data['title'] = message.title
        if hasattr(message, 'selftext'):
            data['selftext'] = message.selftext
        if hasattr(message, 'num_comments'):
            data['num_comments'] = message.num_comments
        if hasattr(message, 'subreddit'):
            data['subreddit'] = message.subreddit
        if hasattr(message, 'parent_id'):
            data['parent_id'] = message.parent_id
        if hasattr(message, 'depth'):
            data['depth'] = message.depth

        doc_ref = db.collection(STOCK_DATA_COLLECTION).document(message.id)
        # Always update the document with merge=True
        await doc_ref.set(data, merge=True)
        logger.debug(f"Updated message {message.id} in Firestore")
        return True
    except Exception as e:
        logger.error(f"Failed to update message {message.id}: {str(e)}", exc_info=True)
        return False
