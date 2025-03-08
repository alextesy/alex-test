import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime
import sys
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file first
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Set Firestore collection names and make sure they're available
os.environ['FIRESTORE_STOCK_DATA_COLLECTION'] = 'stock_data'
os.environ['FIRESTORE_SCRAPER_STATE_COLLECTION'] = 'scraper_state'
print(f"\nSetting Firestore collections:")
print(f"FIRESTORE_STOCK_DATA_COLLECTION: {os.environ.get('FIRESTORE_STOCK_DATA_COLLECTION')}")
print(f"FIRESTORE_SCRAPER_STATE_COLLECTION: {os.environ.get('FIRESTORE_SCRAPER_STATE_COLLECTION')}")

# Add the parent directory to the Python path so we can import our modules
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# Patch PRAW's _load_config method before importing any PRAW-dependent modules
import praw
original_load_config = praw.config.Config._load_config

# Create a simplified _load_config method that doesn't try to load from files
@classmethod
def patched_load_config(cls, **kwargs):
    pass

# Apply the patch
praw.config.Config._load_config = patched_load_config

# Mock only Firebase modules
sys.modules['firebase_functions'] = MagicMock()
sys.modules['firebase_admin'] = MagicMock()

# Mock firebase_functions before importing main
firebase_functions_mock = MagicMock()
firebase_functions_mock.https = MagicMock()
firebase_functions_mock.https.on_request = lambda x: x
firebase_functions_mock.Request = MagicMock()
firebase_functions_mock.Response = MagicMock()

# Mock firebase_admin
firebase_admin_mock = MagicMock()
firebase_admin_mock.initialize_app = MagicMock()
firebase_admin_mock.firestore = MagicMock()

# Fix the test by importing main in a specific order to ensure environment variables are set
# This needs to be done after all the mocking
with patch.dict('sys.modules', {
    'firebase_functions': firebase_functions_mock,
    'firebase_admin': firebase_admin_mock
}):
    # Now we can import the functions from main
    from main import scrape_reddit, STOCK_DATA_COLLECTION, SCRAPER_STATE_COLLECTION
    print(f"Imported main module with collections: {STOCK_DATA_COLLECTION}, {SCRAPER_STATE_COLLECTION}")

# Reset the patch after imports to avoid affecting other modules
praw.config.Config._load_config = original_load_config

class MockFirestore:
    def __init__(self):
        self.stored_messages = {}
        self.scraper_state = {}
        
    def collection(self, name):
        return MockCollection(self, name)

class MockCollection:
    def __init__(self, db, name):
        self.db = db
        self.name = name
        
    def document(self, doc_id):
        return MockDocument(self.db, self.name, doc_id)

class MockDocument:
    def __init__(self, db, collection_name, doc_id):
        self.db = db
        self.collection_name = collection_name
        self.doc_id = doc_id
        print(f"Created MockDocument for collection {collection_name}, doc_id {doc_id}")
        
    async def get(self):
        if self.collection_name == SCRAPER_STATE_COLLECTION:
            data = self.db.scraper_state.get(self.doc_id, {})
            print(f"Getting state for {self.collection_name}/{self.doc_id}: {data}")
            return MockDocumentSnapshot(data)
        return MockDocumentSnapshot({})
        
    async def set(self, data):
        print(f"Setting data for {self.collection_name}/{self.doc_id}")
        if self.collection_name == SCRAPER_STATE_COLLECTION:
            self.db.scraper_state[self.doc_id] = data
            print(f"Storing state in {self.collection_name}: {data}")
        elif self.collection_name == STOCK_DATA_COLLECTION:
            self.db.stored_messages[self.doc_id] = data
            print(f"Storing message in {self.collection_name}: {self.doc_id}")
        return self

class MockDocumentSnapshot:
    def __init__(self, data):
        self.data = data
        self.exists = bool(data)
        
    def get(self, field):
        return self.data.get(field)

@pytest.fixture
def mock_firestore():
    return MockFirestore()

@pytest.mark.asyncio
async def test_scrape_reddit(mock_firestore):
    """Test Reddit scraping functionality with real scraper"""
    print("\nStarting Reddit scraper test...")
    
    # Create a direct instance of RedditScraper to test its functionality
    from scrapers.reddit_scraper import RedditScraper
    reddit_scraper = RedditScraper()
    
    # Test getting posts from a subreddit directly
    subreddit_name = "stocks"
    print(f"\nTesting direct scraping from r/{subreddit_name}...")
    posts = reddit_scraper.get_subreddit_posts(subreddit_name, limit=3)
    
    # Print the posts we got
    print(f"\nScraped {len(posts)} posts from r/{subreddit_name}")
    for i, post in enumerate(posts[:3]):  # Limit to first 3 posts to avoid too much output
        print(f"\nPost {i+1}:")
        print(f"  Title: {post.title[:50]}..." if hasattr(post, 'title') else "  No title")
        print(f"  Author: {post.author}")
        print(f"  Score: {post.score}")
        print(f"  URL: {post.url}")
    
    # Basic assertion just on the scraping functionality
    assert len(posts) > 0, "Should have scraped at least one post"
    
    # Check post fields to verify structure
    for post in posts:
        assert post.id, "Post should have ID"
        assert post.author, "Post should have author"
        assert hasattr(post, 'subreddit'), "Post should have subreddit field"
    
    print("\nReddit scraping test passed!")
    return True

@pytest.mark.asyncio
async def test_daily_discussion(mock_firestore):
    """Test Reddit daily discussion functionality with real scraper"""
    print("\nStarting Reddit daily discussion test...")
    
    # Create a direct instance of RedditScraper to test its functionality
    from scrapers.reddit_scraper import RedditScraper
    reddit_scraper = RedditScraper()
    
    # Test getting the daily discussion thread from wallstreetbets
    print("\nTesting daily discussion thread retrieval...")
    discussion_post, comments = reddit_scraper.get_daily_discussion_comments(limit=3)
    
    # Print information about the discussion thread
    if discussion_post:
        print(f"\nFound daily discussion thread:")
        print(f"  Title: {discussion_post.title[:80]}...")
        print(f"  Posted by: {discussion_post.author}")
        print(f"  Comment count: {discussion_post.num_comments}")
        print(f"  Retrieved comments: {len(comments)}")
        
        # Print a few sample comments if available
        if comments:
            print(f"\nSample comments:")
            for i, comment in enumerate(comments[:3]):  # Limit to first 3 comments
                print(f"\nComment {i+1}:")
                print(f"  Author: {comment.author}")
                print(f"  Content: {comment.content[:50]}..." if comment.content else "  No content")
                print(f"  Score: {comment.score}")
    else:
        print("\nNo daily discussion thread found")
    
    # Basic assertions on the discussion thread
    if discussion_post:
        assert discussion_post.id, "Discussion post should have ID"
        assert discussion_post.title and "Discussion Thread" in discussion_post.title, "Title should contain 'Discussion Thread'"
        assert discussion_post.author, "Discussion post should have author"
        assert discussion_post.subreddit == "wallstreetbets", "Discussion should be from wallstreetbets"
        
        # Assertions on comments
        if comments:
            assert len(comments) > 0, "Should have retrieved at least one comment"
            for comment in comments:
                assert comment.id, "Comment should have ID"
                assert comment.author, "Comment should have author"
                assert hasattr(comment, 'parent_id'), "Comment should have parent_id field"

def main():
    """Main function to run tests manually"""
    # Run tests
    print("Running Reddit scraper test...")
    asyncio.run(test_scrape_reddit(MockFirestore()))
    asyncio.run(test_daily_discussion(MockFirestore()))
    
if __name__ == "__main__":
    main() 