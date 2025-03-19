import os
import logging
import asyncio
from datetime import datetime
from google.cloud import firestore

# Get logger
logger = logging.getLogger(__name__)

# Get collection names from environment
STOCK_DATA_COLLECTION = os.getenv('FIRESTORE_STOCK_DATA_COLLECTION')
SCRAPER_STATE_COLLECTION = os.getenv('FIRESTORE_SCRAPER_STATE_COLLECTION')

# Set defaults if not found
if not STOCK_DATA_COLLECTION:
    STOCK_DATA_COLLECTION = 'stock_data'
    logger.warning(f"FIRESTORE_STOCK_DATA_COLLECTION not set, using default: {STOCK_DATA_COLLECTION}")

if not SCRAPER_STATE_COLLECTION:
    SCRAPER_STATE_COLLECTION = 'scraper_state'
    logger.warning(f"FIRESTORE_SCRAPER_STATE_COLLECTION not set, using default: {SCRAPER_STATE_COLLECTION}")

# Import models and scrapers inside functions to avoid initialization errors
# that could prevent the container from starting

async def store_message_in_firestore(message, db):
    """Store a message in Firestore"""
    try:
        # Import here to avoid initialization errors
        from models.message import Message, RedditPost, RedditComment
        
        # Check if document already exists
        doc_ref = db.collection(STOCK_DATA_COLLECTION).document(message.id)
        doc = await doc_ref.get()
        
        if doc.exists:
            logger.debug(f"Message {message.id} already exists in Firestore, skipping")
            return True
            
        # Convert message to dict for Firestore storage
        base_data = {
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
        
        # Twitter handling commented out
        # if isinstance(message, Tweet):
        #     base_data.update({
        #         'retweet_count': message.retweet_count,
        #         'favorite_count': message.favorite_count
        #     })
        if isinstance(message, (RedditPost, RedditComment)):
            base_data.update({
                'title': message.title,
                'selftext': getattr(message, 'selftext', ''),
                'num_comments': message.num_comments,
                'subreddit': message.subreddit
            })
            if isinstance(message, RedditComment):
                base_data.update({
                    'parent_id': message.parent_id,
                    'depth': message.depth
                })
        
        # Store in Firestore using configured collection name
        await doc_ref.set(base_data)
        logger.debug(f"Stored new message {message.id} in Firestore collection {STOCK_DATA_COLLECTION}")
        return True
    except Exception as e:
        logger.error(f"Failed to store message {message.id} in Firestore: {str(e)}")
        return False

async def scrape_reddit(db, limit=None):
    """Scrape Reddit posts and comments"""
    logger.info("Starting Reddit scraping process")
    
    try:
        # Import here to avoid initialization errors
        from models.message import Message, RedditPost, RedditComment
        from scrapers.reddit_scraper import RedditScraper
        
        stored_count = 0
        skipped_count = 0
        new_replies_count = 0
        
        # Wrap the entire function in a try-except to catch any errors
        try:
            async with RedditScraper() as reddit_scraper:
                state_ref = db.collection(SCRAPER_STATE_COLLECTION).document('reddit')
                state_doc = await state_ref.get()
                
                # Get last discussion ID and last check time from Firestore
                last_discussion_id = None
                last_check_time = None
                
                if state_doc.exists:
                    last_discussion_id = state_doc.get('last_daily_discussion_id')
                    # Get last_updated timestamp if it exists
                    last_updated = state_doc.get('last_updated')
                    if last_updated:
                        # Convert to timestamp if it's a datetime
                        if isinstance(last_updated, datetime):
                            last_check_time = last_updated.timestamp()
                        else:
                            # Try to parse as timestamp
                            try:
                                last_check_time = last_updated.timestamp()
                            except AttributeError:
                                # If it's already a timestamp or something else
                                last_check_time = None
                
                logger.info(f"Found last discussion ID: {last_discussion_id}" if last_discussion_id else "No previous daily discussion ID found")
                logger.info(f"Found last check time: {datetime.fromtimestamp(last_check_time)}" if last_check_time else "No previous check time found")

                # Try to get today's daily discussion thread with retries
                logger.info("Fetching daily discussion thread and comments")
                daily_post = None
                daily_comments = []
                
                try:
                    # Get daily discussion thread
                    daily_post, daily_comments = await reddit_scraper.get_daily_discussion_comments(
                        limit=limit, 
                        last_discussion_id=last_discussion_id,
                        last_check_time=last_check_time
                    )
                except Exception as e:
                    logger.error(f"Failed to get daily discussion comments: {str(e)}", exc_info=True)
                    return 0
                
                if daily_post is None:
                    logger.info("No new daily discussion thread found and no new comments in existing thread")
                    return 0
                
                # If we got here, we have either a new daily discussion thread or new comments
                logger.info(f"Found daily discussion thread: {daily_post.title} with {len(daily_comments)} comments to process")
                
                # Check if post already exists before storing
                post_ref = db.collection(STOCK_DATA_COLLECTION).document(daily_post.id)
                post_doc = await post_ref.get()
                
                if not post_doc.exists:
                    # Store the post itself only if it doesn't exist
                    if await store_message_in_firestore(daily_post, db):
                        stored_count += 1
                        logger.info(f"Stored new daily discussion post: {daily_post.id}")
                else:
                    logger.info(f"Daily discussion post {daily_post.id} already exists, updating stats")
                    # Update the post stats even if it exists
                    if await store_message_in_firestore(daily_post, db):
                        logger.info(f"Updated stats for daily discussion post: {daily_post.id}")
                        skipped_count += 1
                    else:
                        logger.warning(f"Failed to update stats for daily discussion post: {daily_post.id}")
                        skipped_count += 1
                
                # Organize comments by their depth for hierarchical processing
                comments_by_depth = {}
                for comment in daily_comments:
                    depth = comment.depth
                    if depth not in comments_by_depth:
                        comments_by_depth[depth] = []
                    comments_by_depth[depth].append(comment)
                
                # Process comments level by level, starting from top-level comments
                for depth in sorted(comments_by_depth.keys()):
                    level_comments = comments_by_depth[depth]
                    logger.debug(f"Processing {len(level_comments)} comments at depth {depth}")
                    
                    try:
                        # Batch check for existing comments at this level
                        comment_refs = {comment.id: db.collection(STOCK_DATA_COLLECTION).document(comment.id) 
                                      for comment in level_comments}
                        
                        # Get all comment docs in parallel
                        comment_docs = await asyncio.gather(
                            *[ref.get() for ref in comment_refs.values()],
                            return_exceptions=True
                        )
                        
                        # Create a map of comment_id to existence and current data
                        existing_comments = {}
                        for comment_id, doc in zip(comment_refs.keys(), comment_docs):
                            if isinstance(doc, Exception):
                                existing_comments[comment_id] = {'exists': True, 'data': None}  # Treat errors as existing to be safe
                            else:
                                existing_comments[comment_id] = {
                                    'exists': doc.exists,
                                    'data': doc.to_dict() if doc.exists else None
                                }
                        
                        # Store comments at this level
                        for comment in level_comments:
                            try:
                                comment_status = existing_comments.get(comment.id, {'exists': False, 'data': None})
                                
                                if not comment_status['exists']:
                                    # New comment, store it
                                    if await store_message_in_firestore(comment, db):
                                        stored_count += 1
                                        logger.debug(f"Stored new Reddit comment {comment.id} at depth {depth}")
                                else:
                                    # Comment exists, check if it needs updating
                                    existing_data = comment_status['data']
                                    if existing_data:
                                        # Compare relevant fields to see if the comment has been updated
                                        if (existing_data.get('score') != comment.score or 
                                            existing_data.get('content') != comment.content):
                                            # Update the comment if it has changed
                                            if await store_message_in_firestore(comment, db):
                                                new_replies_count += 1
                                                logger.debug(f"Updated existing comment {comment.id} at depth {depth}")
                                        else:
                                            skipped_count += 1
                                            logger.debug(f"Comment {comment.id} at depth {depth} exists and unchanged, skipping")
                                    else:
                                        skipped_count += 1
                                        logger.debug(f"Comment {comment.id} at depth {depth} exists but data unavailable, skipping")
                                        
                            except Exception as e:
                                logger.error(f"Failed to process Reddit comment {comment.id}: {str(e)}")
                                continue
                    except Exception as e:
                        logger.error(f"Failed to process comments at depth {depth}: {str(e)}")
                        continue
                
                # Store the new daily discussion ID and update timestamp in Firestore
                try:
                    current_time = datetime.now()
                    await state_ref.set({
                        'last_daily_discussion_id': daily_post.id,
                        'last_updated': current_time
                    })
                    logger.info(f"Updated state document with ID {daily_post.id} and timestamp {current_time}")
                except Exception as e:
                    logger.error(f"Failed to update state document: {str(e)}")
                        
                logger.info(
                    f"Scraping summary:\n"
                    f"- New items stored: {stored_count}\n"
                    f"- Existing items skipped: {skipped_count}\n"
                    f"- Comments updated: {new_replies_count}\n"
                    f"- Total items processed: {stored_count + skipped_count}"
                )
                return stored_count
                    
        except Exception as e:
            logger.error(f"Error in Reddit scraping process: {str(e)}", exc_info=True)
            return 0
            
    except ImportError as e:
        # If we can't import the required modules, log the error and return 0
        logger.error(f"Failed to import required modules: {str(e)}", exc_info=True)
        return 0
    except Exception as e:
        # Catch any other errors
        logger.error(f"Unexpected error in scrape_reddit: {str(e)}", exc_info=True)
        return 0 