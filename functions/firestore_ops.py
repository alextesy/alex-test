import os
import logging
import asyncio
from datetime import datetime

from google.cloud import firestore

logger = logging.getLogger(__name__)

# Get collection names from environment or use defaults
STOCK_DATA_COLLECTION = os.getenv('FIRESTORE_STOCK_DATA_COLLECTION', 'stock_data')
SCRAPER_STATE_COLLECTION = os.getenv('FIRESTORE_SCRAPER_STATE_COLLECTION', 'scraper_state')


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


async def scrape_reddit(db: firestore.Client, limit=None) -> int:
    """
    Refactored function to scrape Reddit using the new decoupled scraper module.
    This version unconditionally updates posts and comments in Firestore.
    
    Args:
        db: The Firestore client.
        limit: Optional limit for the number of comments to fetch.
    
    Returns:
        int: The total number of messages updated.
    """
    logger.info("Starting Reddit scraping process")
    stored_count = 0

    try:
        # Import the new RedditScraper from the refactored module.
        from scrapers.reddit_scraper_v2 import RedditScraper

        async with RedditScraper() as reddit_scraper:
            # Retrieve scraper state from Firestore
            state_ref = db.collection(SCRAPER_STATE_COLLECTION).document('reddit')
            state_doc = await state_ref.get()
            last_discussion_id = None
            last_check_time = None

            if state_doc.exists:
                last_discussion_id = state_doc.get('last_daily_discussion_id')
                last_updated = state_doc.get('last_updated')
                if last_updated:
                    if isinstance(last_updated, datetime):
                        last_check_time = last_updated.timestamp()
                    else:
                        try:
                            last_check_time = last_updated.timestamp()
                        except Exception:
                            last_check_time = None

            logger.info(last_discussion_id
                        and f"Found last discussion ID: {last_discussion_id}"
                        or "No previous discussion state found")
            logger.info(last_check_time
                        and f"Last check time: {datetime.fromtimestamp(last_check_time)}"
                        or "No previous check time found")

            # Fetch the daily discussion thread (or new comments if it's an existing thread)
            daily_post, daily_comments = await reddit_scraper.fetch_daily_discussion(
                limit=limit,
                last_discussion_id=last_discussion_id,
                last_check_time=last_check_time
            )

            if daily_post is None:
                logger.info("No new daily discussion thread found or no new comments available")
                return 0

            logger.info(f"Found daily discussion thread: {daily_post.title} with {len(daily_comments)} comments to process")

            # Update the daily discussion post unconditionally
            if await store_message_in_firestore(daily_post, db):
                stored_count += 1
                logger.info(f"Updated daily discussion post: {daily_post.id}")

            # Update each comment unconditionally
            for comment in daily_comments:
                if await store_message_in_firestore(comment, db):
                    stored_count += 1
                    logger.debug(f"Updated comment: {comment.id}")

            # Update the state document with the latest discussion thread info
            current_time = datetime.now()
            await state_ref.set({
                'last_daily_discussion_id': daily_post.id,
                'last_updated': current_time
            })
            logger.info(f"Updated state document with daily discussion ID {daily_post.id} at {current_time}")

        logger.info(f"Scraping summary: Updated {stored_count} items in Firestore")
        return stored_count

    except Exception as e:
        logger.error(f"Error in scrape_reddit: {str(e)}", exc_info=True)
        return 0
