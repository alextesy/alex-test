import logging
import json
from datetime import datetime
from time import sleep
from db.database import SessionLocal
from models.database_models import RawMessage, ProcessedMessage
from processors.message_processor import MessageProcessor
from models.message import Tweet, RedditPost, RedditComment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_message_from_raw(raw_msg: RawMessage):
    """Create appropriate Message object from raw message"""
    raw_data = json.loads(raw_msg.raw_data)
    
    base_args = {
        'id': raw_msg.id,
        'content': raw_msg.content,
        'author': raw_msg.author,
        'timestamp': raw_msg.timestamp,
        'url': raw_msg.url,
        'score': raw_msg.score,
        'platform': raw_msg.platform,
        'source': raw_msg.source
    }
    
    if raw_msg.platform == "twitter":
        return Tweet(
            **base_args,
            retweet_count=raw_data.get("retweet_count", 0),
            favorite_count=raw_data.get("favorite_count", 0)
        )
    elif raw_msg.platform == "reddit":
        if raw_msg.parent_id:  # It's a comment
            return RedditComment(
                **base_args,
                title=raw_msg.title,
                selftext=raw_data.get("selftext", ""),
                num_comments=raw_data.get("num_comments", 0),
                subreddit=raw_data.get("subreddit", ""),
                parent_id=raw_msg.parent_id,
                depth=raw_data.get("depth", 0)
            )
        else:  # It's a post
            return RedditPost(
                **base_args,
                title=raw_msg.title,
                selftext=raw_data.get("selftext", ""),
                num_comments=raw_data.get("num_comments", 0),
                subreddit=raw_data.get("subreddit", "")
            )

def process_messages():
    """Main processing loop"""
    message_processor = MessageProcessor()
    
    while True:
        try:
            db = SessionLocal()
            
            # Get unprocessed messages
            raw_messages = db.query(RawMessage).filter_by(processed=False).limit(100).all()
            
            if not raw_messages:
                logger.info("No new messages to process")
                sleep(60)
                continue
            
            # Convert raw messages to Message objects
            messages = []
            for raw_msg in raw_messages:
                try:
                    message = create_message_from_raw(raw_msg)
                    messages.append(message)
                except Exception as e:
                    logger.error(f"Error creating message object for {raw_msg.id}: {str(e)}")
                    continue
            
            # Process messages in batch
            processed_messages = message_processor.process_messages(messages)
            
            # Save processed messages to database
            for message, raw_msg in zip(processed_messages, raw_messages):
                try:
                    # Convert to database model and save
                    db_message = message.to_db_model(db)
                    db.add(db_message)
                    
                    # Mark raw message as processed
                    raw_msg.processed = True
                    
                except Exception as e:
                    logger.error(f"Error saving processed message {message.id}: {str(e)}")
                    continue
            
            db.commit()
            logger.info(f"Processing completed at {datetime.now()}")
            
        except Exception as e:
            logger.error(f"Error during processing: {str(e)}")
            db.rollback()
        
        finally:
            db.close()
        
        # Small delay before next batch
        sleep(10)

if __name__ == "__main__":
    logger.info("Starting processor service...")
    process_messages() 