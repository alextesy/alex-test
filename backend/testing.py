from scraper_service.src.scrapers.cnbc_scraper import CNBCScraper
from scraper_service.src.scrapers.reddit_scraper import RedditScraper
import sys
sys.path.append('../')

def test_cnbc_scraper():
    """Test CNBC scraper with TSLA query"""
    scraper = CNBCScraper()
    articles = scraper.get_posts("TSLA", limit=5)  # Limiting to 5 articles for testing
    
    print(f"\nFound {len(articles)} articles about TSLA:")
    for article in articles:
        print("\n-------------------")
        print(f"Title: {article.title}")
        print(f"Author: {article.author} ({article.author_title})")
        print(f"Category: {article.category}")
        print(f"URL: {article.url}")
        print(f"Summary: {article.summary[:200]}...")  # First 200 chars of summary
        print("-------------------")

def test_reddit_daily_discussion():
    """Test Reddit scraper's daily discussion retrieval"""
    scraper = RedditScraper()
    post, comments = scraper.get_daily_discussion_comments(limit=50)
    
    if post:
        print("\nFound Daily Discussion Thread:")
        print("-------------------")
        print(f"Title: {post.title}")
        print(f"Author: {post.author}")
        print(f"Score: {post.score}")
        print(f"URL: {post.url}")
        print(f"Number of comments retrieved: {len(comments)}")
        print("-------------------")
        
        print("\nFirst 5 comments:")
        for i, comment in enumerate(comments[:5], 1):
            print(f"\nComment {i}:")
            print(f"Author: {comment.author}")
            print(f"Score: {comment.score}")
            print(f"Content: {comment.content[:200]}...")  # First 200 chars of content
            print(f"Depth: {comment.depth}")
    else:
        print("\nNo Daily Discussion Thread found for today")

if __name__ == "__main__":
    # test_cnbc_scraper()
    test_reddit_daily_discussion()