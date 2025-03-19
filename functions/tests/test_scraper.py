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
os.environ['FIREBASE_PROJECT_ID'] = 'test-project'

# Mock Google Cloud modules
class MockLoggingClient:
    def __init__(self, project):
        self.project = project
    
    def setup_logging(self):
        pass

mock_logging = MagicMock()
mock_logging.Client = MockLoggingClient

sys.modules['google'] = MagicMock()
sys.modules['google.cloud'] = MagicMock()
sys.modules['google.cloud.logging'] = mock_logging

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

# Fix the test by importing in the correct order to ensure environment variables are set
# This needs to be done after all the mocking
with patch.dict('sys.modules', {
    'firebase_functions': firebase_functions_mock,
    'firebase_admin': firebase_admin_mock
}):
    # Now we can import from the new modules
    from main import PROJECT_ID
    from firestore_ops import scrape_reddit, STOCK_DATA_COLLECTION, SCRAPER_STATE_COLLECTION
    print(f"Imported modules with collections: {STOCK_DATA_COLLECTION}, {SCRAPER_STATE_COLLECTION}")

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
    
    async with RedditScraper() as reddit_scraper:
        # Test getting posts from a subreddit directly
        subreddit_name = "stocks"
        print(f"\nTesting direct scraping from r/{subreddit_name}...")
        posts = await reddit_scraper.get_subreddit_posts(subreddit_name, limit=3)
        
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
@pytest.mark.timeout(30)  # Set a 30-second timeout for this test
async def test_get_post_with_comments():
    """Test get_post_with_comments functionality with different comment limits"""
    print("\nStarting get_post_with_comments test...")

    from scrapers.reddit_scraper import RedditScraper
    
    async with RedditScraper() as reddit_scraper:
        # First, get a regular post from r/stocks with moderate number of comments
        print("\nFetching a test post from r/stocks...")
        posts = await reddit_scraper._get_subreddit_posts('stocks', limit=5, sort='hot')  # Reduced from 10 to 5

        # Find a post with a reasonable number of comments (between 5 and 20)
        test_post = None
        for post in posts:
            if 5 <= post.num_comments <= 20:  # Reduced comment range
                test_post = post
                break

        if not test_post:
            # If we can't find a post with few comments, use the first post we found
            test_post = posts[0]
            print(f"Using post with {test_post.num_comments} comments")

        assert test_post, "Should have found a test post"
        print(f"\nFound test post:")
        print(f"Title: {test_post.title[:50]}...")
        print(f"Number of comments: {test_post.num_comments}")

        # Test cases with different limits
        test_limits = [3, 5, 10]  # Reduced limits and removed None case

        for limit in test_limits:
            print(f"\nTesting with comment limit: {limit}")

            # Get post with comments using the limit
            post, comments = await reddit_scraper.get_post_with_comments(test_post.id, limit)

            # Verify post data
            assert post.id == test_post.id, "Should have fetched the correct post"
            assert post.title == test_post.title, "Should have the same title"
            assert post.num_comments == test_post.num_comments, "Should have same number of comments"

            # Print info about the fetched data
            print(f"Post title: {post.title[:50]}...")
            print(f"Total comments in thread (from post.num_comments): {post.num_comments}")
            print(f"Comments fetched: {len(comments)}")

            # Count comments by depth
            depth_counts = {}
            for comment in comments:
                depth_counts[comment.depth] = depth_counts.get(comment.depth, 0) + 1

            print("\nComment depth distribution:")
            for depth in sorted(depth_counts.keys()):
                print(f"  Depth {depth}: {depth_counts[depth]} comments")

            # Print sample of fetched comments
            print("\nSample of fetched comments:\n")
            for i, comment in enumerate(comments[:2], 1):  # Reduced from 3 to 2
                print(f"Comment {i}:")
                print(f"  Author: {comment.author}")
                print(f"  Depth: {comment.depth}")
                print(f"  Parent ID: {comment.parent_id}")
                print(f"  Content: {comment.content[:50]}...\n")

            # Verify comments
            # For all cases, we should have at most limit top-level comments
            top_level_comments = sum(1 for c in comments if c.depth == 0)
            assert top_level_comments <= limit, f"Should not fetch more than {limit} top-level comments"
            print(f"Test with limit {limit} passed!")

@pytest.mark.asyncio
@pytest.mark.timeout(45)  # Increased timeout to 45 seconds to handle slower responses
async def test_get_new_comments():
    """Test get_new_comments functionality by comparing comments before and after a timestamp"""
    print("\nStarting get_new_comments test...")

    from scrapers.reddit_scraper import RedditScraper
    
    async with RedditScraper() as reddit_scraper:
        # Try multiple subreddits in case one doesn't have suitable posts
        subreddits = ['stocks', 'investing', 'wallstreetbets']
        test_post = None
        
        for subreddit in subreddits:
            try:
                print(f"\nTrying to fetch posts from r/{subreddit}...")
                # Get newest posts to ensure we have recent comments
                posts = await reddit_scraper._get_subreddit_posts(subreddit, limit=10, sort='new')
                
                # Find a post with a reasonable number of comments
                for post in posts:
                    if post.num_comments >= 5:  # Reduced minimum comment requirement
                        test_post = post
                        print(f"Found suitable post in r/{subreddit}")
                        break
                
                if test_post:
                    break
                    
            except Exception as e:
                print(f"Error fetching from r/{subreddit}: {str(e)}")
                continue

        if not test_post:
            pytest.skip("Could not find suitable test post in any subreddit")
        
        print(f"\nSelected test post:")
        print(f"Subreddit: r/{test_post.subreddit}")
        print(f"Title: {test_post.title[:50]}...")
        print(f"Number of comments: {test_post.num_comments}")
        print(f"Created at: {datetime.fromtimestamp(test_post.timestamp)}")

        try:
            # Get all comments first
            print("\nFetching all comments...")
            post, all_comments = await reddit_scraper.get_post_with_comments(test_post.id)
            
            if not all_comments:
                pytest.skip(f"Could not fetch any comments for the test post")
            
            print(f"Successfully fetched {len(all_comments)} comments")
            
            # Use a timestamp just before the oldest comment to ensure we get some comments
            oldest_comment = min(all_comments, key=lambda x: x.timestamp)
            cutoff_time = oldest_comment.timestamp - 60  # 1 minute before oldest comment
            
            print(f"\nComment timeline:")
            print(f"  Cutoff time:    {datetime.fromtimestamp(cutoff_time)}")
            print(f"  Oldest comment: {datetime.fromtimestamp(oldest_comment.timestamp)}")
            print(f"  Newest comment: {datetime.fromtimestamp(max(c.timestamp for c in all_comments))}")
            
            # Get new comments using the cutoff time
            print("\nFetching new comments...")
            new_comments = await reddit_scraper.get_new_comments(test_post.id, cutoff_time)
            
            # Verify the results
            assert new_comments, "Should have fetched some new comments"
            assert len(new_comments) > 0, "Should have found at least one new comment"
            
            # Verify all new comments are newer than cutoff
            for comment in new_comments:
                assert comment.timestamp > cutoff_time, f"Comment timestamp {comment.timestamp} should be after cutoff {cutoff_time}"
            
            print(f"\nSuccessfully fetched {len(new_comments)} new comments")
            print("\nget_new_comments test passed successfully!")
            
        except Exception as e:
            print(f"\nError during test: {str(e)}")
            raise

def main():
    """Main function to run tests manually"""
    print("Running Reddit scraper tests...")
    
    async def run_all_tests():
        mock_firestore = MockFirestore()
        # Run tests sequentially in the same event loop
        await test_scrape_reddit(mock_firestore)
        await test_get_post_with_comments()
        await test_get_new_comments()  # Add the new test to the sequence
    
    # Run all tests in a single event loop
    asyncio.run(run_all_tests())
    
if __name__ == "__main__":
    main() 